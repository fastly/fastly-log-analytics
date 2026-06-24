"""Security repository — TLS analysis, bot detection, and request anomalies."""

from __future__ import annotations

import logging
import os

import duckdb

from backend.models.common import FiltersDict
from backend.repositories._base import (
    QueryRunner,
    SectionTimer,
    _safe_table,
    empty_schema_response,
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
        return empty_schema_response(bots=[], ngwaf_bots=[])

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
                limit=50000,
                per_field_limits={"ua": 50000},
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
            from backend import config as svcconfig

            ngwaf_db = svcconfig.ngwaf_db_path()
            if ngwaf_db:
                existing = con.execute(
                    "SELECT path FROM duckdb_databases() WHERE database_name='ngwaf_top' LIMIT 1"
                ).fetchone()
                already_path = existing[0] if existing else None
                if already_path == ngwaf_db:
                    ngwaf_attached = True
                elif os.path.exists(ngwaf_db):
                    if already_path is not None:
                        try:
                            con.execute("DETACH ngwaf_top")
                        except Exception:
                            pass
                    ngwaf_db_escaped = ngwaf_db.replace("'", "''")
                    con.execute(f"ATTACH '{ngwaf_db_escaped}' AS ngwaf_top (TYPE SQLITE, READ_ONLY)")
                    ngwaf_attached = True
        except Exception:
            pass  # ATTACH failed — fall back gracefully

    needs_filtered_ua_scan = ua_rollup_rows is None and "ua" in actual_cols
    cols_needed: list[str] = []
    if needs_filtered_ua_scan:
        cols_needed.append("ua")
    if ngwaf_attached and "waf_req_id" in actual_cols:
        cols_needed.append("waf_req_id")

    if cols_needed:
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
                    q = SQL.TOP_UAS_BY_COUNT.format(temp_table=temp_table)
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
        return empty_schema_response(
            tls_fingerprints=[],
            req_size_dist=[],
            ipv6_adoption=[],
            proxy_dist=[],
            conn_reuse_dist=[],
            http_versions=[],
            section_timings=section_timings,
            **runner.telemetry(),
        )

    _t = _time.perf_counter()
    params, where_clause = build_where_clause(start_time, end_time, filters, actual_cols, inline_params=True)
    timer.mark("build_where_clause", _t)

    # Pre-check rollup availability so the catalog temp can DROP the
    # corresponding column when the aggregate it feeds will serve from
    # the count rollup instead of scanning the temp. Pre-checking is a
    # cheap directory-stat (no parquet reads) so it's safe to run before
    # materialization. The invariant the live-fallback in
    # _build_security_response relies on: "rollup pre-check passed → col
    # dropped from temp AND rollup serves the aggregate; pre-check failed
    # → col present in temp AND live SQL serves". The two conditions are
    # gated on the SAME pre-check so the fallback never references a
    # column the temp doesn't have.
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

    # Projection narrowed: asn / req_bytes / ja3 / ja4 are not consumed
    # by _build_security_response (audited 2026-06-05) so they're dropped
    # from the TEMP TABLE materialization. Each saves a column scan +
    # cast per parquet read. is_ipv6 / p_type also drop conditionally
    # when their rollup-served paths fire (#94 closure, 2026-06-15).
    cols = [
        "timestamp",
        "ip",
        "tls_ciphers_sha",
        "req_header_bytes",
        *(["is_ipv6"] if not use_ipv6_rollup else []),
        *(["p_type"] if not use_proxy_rollup else []),
        "conn_requests",
        "waf_sig",
        "ua",
        "waf_req_id",
    ]
    _t = _time.perf_counter()
    temp_table = runner.create_filtered_temp_table(cols, actual_cols, table_name, where_clause, params)
    timer.mark("temp_table_create", _t)
    if temp_table is None:
        return {"section_timings": section_timings, **runner.telemetry()}

    try:
        return _build_security_response(
            runner,
            src,
            con,
            actual_cols,
            temp_table,
            bucket_seconds,
            section_timings,
            start_time=start_time,
            end_time=end_time,
            filters=filters,
            use_ipv6_rollup=use_ipv6_rollup,
            use_proxy_rollup=use_proxy_rollup,
            sections=sections,
        )
    finally:
        try:
            runner.execute(f'DROP TABLE IF EXISTS "{temp_table}"')
        except Exception:
            pass


def _build_security_response(
    runner: QueryRunner,
    src: dict,
    con: duckdb.DuckDBPyConnection,
    actual_cols: list[str],
    temp_table: str,
    bucket_seconds: int,
    section_timings: list[dict] | None = None,
    *,
    start_time: str | None = None,
    end_time: str | None = None,
    filters: FiltersDict | None = None,
    use_ipv6_rollup: bool = False,
    use_proxy_rollup: bool = False,
    sections: set[str] | None = None,
) -> dict:
    import time as _time

    timer = SectionTimer(section_timings)
    section_timings = timer.entries

    def _want(name: str) -> bool:
        return sections is None or name in sections

    results = {**runner.telemetry()}

    # Surface whether NGWAF is configured so the frontend can distinguish
    # "not configured" from "configured but no detections yet". Bundled
    # with the NGWAF bot section gate — selector callers that want either
    # bot view also want the badge; pure non-bot requests skip the
    # config lookup.
    _want_ngwaf_bots = _want("ngwaf_verified_bots") or _want("ngwaf_verified_bots_ts")
    if _want_ngwaf_bots:
        try:
            from backend import config as svcconfig

            results["ngwaf_configured"] = bool(svcconfig.get_ngwaf_workspace_id(src.get("service_id", "")))
        except Exception:
            results["ngwaf_configured"] = False

    # Attach the NGWAF bot cache once per connection if it exists and waf_req_id is in schema.
    # The attach costs ~22ms; check DuckDB's own duckdb_databases() catalog
    # (~90us) first and skip the ATTACH if this connection already has the
    # cache bound to the exact same path. The catalog query reflects live
    # state, so we don't need Python-side memoization (DuckDBPyConnection
    # has no __dict__ for arbitrary attrs anyway) and a config switch that
    # changes the path triggers a DETACH + re-ATTACH instead of silently
    # serving from a stale binding.
    _ngwaf_attached = False
    if _want_ngwaf_bots and "waf_req_id" in actual_cols:
        try:
            from backend import config as svcconfig

            ngwaf_db = svcconfig.ngwaf_db_path()
            if ngwaf_db:
                existing = con.execute(
                    "SELECT path FROM duckdb_databases() WHERE database_name='ngwaf_cache' LIMIT 1"
                ).fetchone()
                already_path = existing[0] if existing else None
                if already_path == ngwaf_db:
                    _ngwaf_attached = True
                elif os.path.exists(ngwaf_db):
                    if already_path is not None:
                        try:
                            con.execute("DETACH ngwaf_cache")
                        except Exception:
                            pass
                    ngwaf_db_escaped = ngwaf_db.replace("'", "''")
                    con.execute(f"ATTACH '{ngwaf_db_escaped}' AS ngwaf_cache (TYPE SQLITE, READ_ONLY)")
                    _ngwaf_attached = True
        except Exception:
            pass  # ATTACH failed (e.g. DuckDB SQLite extension not loaded) — fall back gracefully

    # 0. Verified Bots Time Series (waf_sig fallback — category-level, no bot names)
    if _want("verified_bots_ts"):
        if "waf_sig" in actual_cols:
            _t = _time.perf_counter()
            # Try the minute-granular verified_bots_ts rollup first
            # (unfiltered, >= 48 h, bucket a multiple of 60). The reader
            # fills the in-progress active hour live from the temp table, so
            # the result is exact and complete to "now". Falls through to the
            # live SQL on any eligibility miss.
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
        else:
            results["verified_bots_ts"] = []

    # 0b. NGWAF Verified Bots (name-resolved, requires ngwaf_bot_cache.db + waf_req_id column)
    if _ngwaf_attached:
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
            results["ngwaf_verified_bots_ts"] = [{"time": safe_iso(r[0]), "bot_name": r[1], "count": r[2]} for r in res]
        except Exception as e:
            import logging

            logging.getLogger(__name__).error("[security] NGWAF bot join failed: %s", e)
            results["ngwaf_verified_bots"] = []
            results["ngwaf_verified_bots_ts"] = []
    elif _want_ngwaf_bots:
        results["ngwaf_verified_bots"] = []
        results["ngwaf_verified_bots_ts"] = []

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

    # Rollup fast-path: when no per-request filters apply, the top-N
    # fingerprints + their cross-window distinct-IP counts can be served
    # from the per-(field, hour) count rollup + the HLL ip_spread rollup
    # instead of the live FINGERPRINT_TOP_N scan over the catalog temp.
    # On 30-day windows this trades a 3 × count(DISTINCT ip) full-temp
    # scan (the dominant cost in security/admin-30d) for two parquet
    # reads + an in-Python HLL merge — the gain the audit's #58-full
    # leg predicted.
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

    # Live FINGERPRINT_TOP_N path: runs for any fingerprint col that
    # the rollup path didn't already serve (either because rollup was
    # bypassed entirely, or because that specific field had no rollup
    # coverage in the window).
    for col, result_key in _FP_RESULT_KEYS:
        if result_key not in _wanted_fp_keys:
            continue
        if col in rollup_served_fp_cols:
            continue
        if col in actual_cols and "ip" in actual_cols:
            q = SQL.FINGERPRINT_TOP_N.format(col=col, temp_table=temp_table)
            _t = _time.perf_counter()
            res = runner.execute(q).fetchall()
            timer.mark(result_key, _t)
            results[result_key] = [{"fingerprint": r[0], "ip_count": r[1], "request_count": r[2]} for r in res]
            coverage_cols.append(col)
        else:
            results[result_key] = []

    # One scan of the temp table populates coverage for every fingerprint
    # card that ran above — replaces three separate full-temp scans (each
    # ~200-400 ms on 30d). Column names come from the _FP_RESULT_KEYS
    # safelist; no untrusted input reaches the inline aggregate list.
    fingerprint_coverage: dict[str, float] = {}
    if coverage_cols and _want("fingerprint_coverage"):
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
            # FE treats 0.0 as "no signal, show the existing emptyMessage"
            # rather than the coverage hint — mirrors the prior fail-soft
            # behaviour of the now-removed _coverage_for helper.
            for c in coverage_cols:
                fingerprint_coverage[c] = 0.0

    if _want("fingerprint_coverage"):
        results["fingerprint_coverage"] = fingerprint_coverage

    # 3. Request Header Size Distribution
    if _want("req_size_dist"):
        if "req_header_bytes" in actual_cols:
            q = SQL.REQ_HEADER_SIZE_DIST.format(temp_table=temp_table)
            _t = _time.perf_counter()
            res = runner.execute(q).fetchall()
            timer.mark("req_size_dist", _t)
            results["req_size_dist"] = [{"bucket": r[0], "count": r[1]} for r in res]
        else:
            results["req_size_dist"] = []

    # Top IPs by Max Header Size
    if _want("top_ips_header"):
        if "req_header_bytes" in actual_cols:
            q = SQL.TOP_IPS_BY_MAX_HEADER.format(temp_table=temp_table)
            _t = _time.perf_counter()
            res = runner.execute(q).fetchall()
            timer.mark("top_ips_by_header", _t)
            results["top_ips_header"] = [{"ip": r[0], "max_header": r[1]} for r in res]
        else:
            results["top_ips_header"] = []

    # 4. IPv6 Adoption over Time
    if _want("ipv6_adoption") and "is_ipv6" in actual_cols:
        if use_ipv6_rollup:
            # Rollup-served path: closed hours from the count rollup +
            # active hour from a focused base-table query. The temp does
            # NOT carry is_ipv6 when this branch runs (get_security_
            # aggregates dropped it), so the live SQL fallback below is
            # NOT available — pre-check passing must mean rollup will
            # serve. _ipv6_per_hour_from_rollups returning None here is
            # a degenerate "files vanished between pre-check and read"
            # race; emit an empty card + warn rather than 500 the
            # request.
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
            q = SQL.IPV6_ADOPTION_TS.format(
                time_bucket_select=time_bucket_select("1 hour"),
                temp_table=temp_table,
            )
            _t = _time.perf_counter()
            res = runner.execute(q).fetchall()
            timer.mark("ipv6_adoption", _t)
            results["ipv6_adoption"] = [{"time": safe_iso(r[0]), "pct": r[1]} for r in res]
    elif _want("ipv6_adoption"):
        results["ipv6_adoption"] = []

    # 5. Proxy/Anonymizer Breakdown
    if _want("proxy_dist") and "p_type" in actual_cols:
        if use_proxy_rollup:
            # Same invariant as ipv6_adoption above: p_type is dropped
            # from the temp when this branch runs, so the live SQL
            # fallback isn't available — None from the helper renders
            # an empty card and logs rather than 500ing.
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
            q = SQL.PROXY_TYPE_DIST.format(temp_table=temp_table)
            _t = _time.perf_counter()
            res = runner.execute(q).fetchall()
            timer.mark("proxy_dist", _t)
            results["proxy_dist"] = [{"type": r[0], "count": r[1]} for r in res]
    elif _want("proxy_dist"):
        results["proxy_dist"] = []

    # 6. Connection Reuse Distribution
    if _want("conn_reuse_dist"):
        if "conn_requests" in actual_cols:
            q = SQL.CONN_REUSE_DIST.format(temp_table=temp_table)
            _t = _time.perf_counter()
            res = runner.execute(q).fetchall()
            timer.mark("conn_reuse_dist", _t)
            results["conn_reuse_dist"] = [{"bucket": r[0], "count": r[1]} for r in res]
        else:
            results["conn_reuse_dist"] = []

    # 7. Well-Known Bots (UA matching + FCrDNS verification)
    if _want("wellknown_bots") and "ua" in actual_cols and "ip" in actual_cols:
        try:
            from backend.core.rollups import read_wellknown_bots_rollup
            from backend.utils.bot_sources import build_matcher, get_bot_regex_pattern
            from backend.utils.rdns_cache import classify, enqueue, get_hostnames

            # Fast path: try to pull (ua, ip, count) tuples from the
            # pre-materialised wellknown_bots rollup. Returns None when
            # any hour in the window lacks a fresh partition (active
            # hour, missing file, or stale pattern_set_version after a
            # bot-source refresh) — the live SQL path below handles
            # those cases correctly. The rollup tuples are the SAME
            # shape the SQL prefilter would have produced, so the
            # Python loop downstream is unchanged.
            _t = _time.perf_counter()
            ua_ip_rows = read_wellknown_bots_rollup(src, start_time, end_time) if (start_time and end_time) else None
            if ua_ip_rows is not None:
                timer.mark("wellknown_bots_rollup_read", _t)
            else:
                # Slow path: regex prefilter against the request-scoped
                # temp_table. Identical to the pre-rollup behaviour;
                # kept as a correctness fallback for hour-mix windows
                # and pattern-set transitions.
                pattern = get_bot_regex_pattern(500)
                if pattern:
                    pattern_sql = pattern.replace("'", "''")
                    prefilter = f"WHERE ua IS NOT NULL AND ip IS NOT NULL AND regexp_matches(ua, '{pattern_sql}')"
                else:
                    prefilter = "WHERE ua IS NOT NULL AND ip IS NOT NULL"

                q = SQL.WELLKNOWN_BOTS_UA_IP.format(temp_table=temp_table, prefilter=prefilter)
                ua_ip_rows = runner.execute(q).fetchall()
                timer.mark("wellknown_bots_query", _t)

            match_ua = build_matcher()
            bot_agg: dict[str, dict] = {}
            new_ips: list[str] = []

            # Batch-resolve every distinct IP in one SELECT instead of opening a
            # fresh SQLite connection per (ua, ip) row inside the loop.
            hostnames = get_hostnames([ip for _, ip, _ in ua_ip_rows if ip])

            for ua_val, ip_val, cnt in ua_ip_rows:
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
    elif _want("wellknown_bots"):
        results["wellknown_bots"] = []

    results["section_timings"] = section_timings
    return results
