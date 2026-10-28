from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal

import duckdb

from backend.core import log_fields as metric_expressions
from backend.core.rollups._common import (
    DAY_BUNDLE_FILENAME,
    NETWORK_QUALITY_ASN_FILENAME,
    NETWORK_QUALITY_COUNTRY_FILENAME,
    NETWORK_QUALITY_POP_FILENAME,
    NETWORK_QUALITY_REGION_FILENAME,
    NETWORK_RTT_BUNDLE_FILENAME,
    NETWORK_SPEED_BUNDLE_FILENAME,
    ORIGIN_IP_BUNDLE_FILENAME,
    ORIGIN_PATH_BUNDLE_FILENAME,
    ORIGIN_POP_BUNDLE_FILENAME,
    ORIGIN_SUMMARY_BUNDLE_FILENAME,
    OVERVIEW_BUNDLE_FILENAME,
    PERF_TOP_ASNS_BUNDLE_FILENAME,
    PERF_TOP_URLS_BUNDLE_FILENAME,
    PERF_TTL_DIST_BUNDLE_FILENAME,
    POP_HEALTH_BUNDLE_FILENAME,
    SECURITY_BOT_EFF_BUNDLE_FILENAME,
    SECURITY_BOT_EFF_UA_TOP_K,
    SECURITY_CONN_REUSE_BUNDLE_FILENAME,
    SECURITY_COV_BUNDLE_FILENAME,
    SECURITY_PROXY_FLAGGED_BUNDLE_FILENAME,
    SECURITY_PROXY_QUALITY_BUNDLE_FILENAME,
    SECURITY_REQ_SIZE_BUNDLE_FILENAME,
    SLOW_URLS_BUNDLE_FILENAME,
    TIME_SERIES_BUNDLE_FILENAME,
    quote_path_list,
)
from backend.core.rollups.recompute import (
    collect_day_prefer_paths,
    collect_hour_paths_with_floor,
    durable_serving_blocked,
    eligible_window,
)
from backend.models.analytics import (
    ReaderCoverage,
    RollupPathCoverage,
)
from backend.repositories.rollup_executor.top_n import CANONICAL_METRICS
from backend.repositories.utils.response_shaping import us_to_ms as _us_to_ms

_logger = logging.getLogger("backend.repositories._base")


class _ScalarsRollupMixin:
    if TYPE_CHECKING:
        con: duckdb.DuckDBPyConnection
        src: dict[str, Any]
        reader_coverage: list[ReaderCoverage]
        _HOUR_START_TS_SQL_EXPR: str

        def execute(self, q: str, p: list[Any] | None = None) -> Any: ...
        def get_schema_cols(self) -> list[str]: ...
        def _create_active_hour_temp_direct(
            self, fields: list[str], actual_cols: list[str] | set[str], live_start: Any, live_end: Any
        ) -> str | None: ...
        def _fetch_active_hour_live_rows(
            self,
            st: datetime,
            et: datetime,
            *,
            required_col: str,
            build_sql: Callable[[str], str],
            log_tag: str,
            actual_cols: list[str] | set[str] | None = None,
            observed: ReaderCoverage | None = None,
        ) -> list[tuple]: ...
        def _plan_scalar_rollup_window(
            self,
            start_time: str | None,
            end_time: str | None,
            *,
            can_use_overview: bool,
            require_minute_aligned: bool,
            observed: ReaderCoverage,
        ) -> tuple[datetime, datetime, Any, str, str, str, str | None] | None: ...
        def _resolve_scalar_live_source(
            self,
            st: datetime,
            et: datetime,
            wplan: Any,
            table_name: str,
            where_clause: str,
            observed: ReaderCoverage,
            *,
            adjust_partial_hour: bool,
            unfiltered_window: bool,
            get_direct_tmp: Callable[[datetime, datetime], str | None],
        ) -> tuple[str | None, int, str | None, bool]: ...
        def _execute_scalar_rollup_sql(
            self,
            final_sql: str,
            params: list,
            *,
            live_needs_params: bool,
            direct_live_tmp: str | None,
            observed: ReaderCoverage,
            log_tag: str,
            fetchone_int: bool = False,
        ) -> Any: ...

    def try_count_from_rollup(
        self,
        start_time: str | None,
        end_time: str | None,
        table_name: str,
        where_clause: str,
        params: list,
        unfiltered_window: bool = False,
        projected_fields: list[str] | None = None,
        actual_cols: list[str] | set[str] | None = None,
    ) -> int | None:
        observed = ReaderCoverage(reader="dashboard.count", eligible=False, outcome="ineligible")
        self.reader_coverage.append(observed)
        if not unfiltered_window:
            observed.eligibility_reason = "filtered"
            return None
        planned = self._plan_scalar_rollup_window(
            start_time, end_time, can_use_overview=True, require_minute_aligned=True, observed=observed
        )
        if planned is None:
            return None
        st, et, wplan, st_tz, st_hour_tz, et_tz, heal_base_table = planned
        select_clauses: list[str] = []
        if wplan.rollup_paths:
            paths_sql = quote_path_list(wplan.rollup_paths)
            select_clauses.append(
                f"SELECT {metric_expressions.sum_or_zero('requests')} AS num "
                f"FROM read_parquet([{paths_sql}]) "
                f"WHERE bucket >= TIMESTAMPTZ '{st_tz}' "
                f"  AND bucket < TIMESTAMPTZ '{et_tz}'"
            )
        if wplan.day_ov_paths:
            ov_paths_sql = quote_path_list(wplan.day_ov_paths)
            hour_ts_expr = self._HOUR_START_TS_SQL_EXPR
            select_clauses.append(
                f"SELECT {metric_expressions.sum_or_zero('requests')} AS num "
                f"FROM read_parquet([{ov_paths_sql}]) "
                f"WHERE {hour_ts_expr} >= TIMESTAMPTZ '{st_hour_tz}' "
                f"  AND {hour_ts_expr} < TIMESTAMPTZ '{et_tz}'"
            )
        if heal_base_table:
            select_clauses.append(f"SELECT COUNT(*) AS num FROM {heal_base_table} WHERE {wplan.heal_where_sql}")
        direct_live_tmp: str | None = None
        live_needs_params = False
        if wplan.crosses_active:
            live_from_where, partial_total, direct_live_tmp, live_needs_params = self._resolve_scalar_live_source(
                st,
                et,
                wplan,
                table_name,
                where_clause,
                observed,
                adjust_partial_hour=True,
                unfiltered_window=unfiltered_window,
                get_direct_tmp=lambda l_start, l_end: self._create_active_hour_temp_direct(
                    projected_fields or [], actual_cols or [], l_start, l_end
                ),
            )
            if live_from_where is not None:
                select_clauses.append(f"SELECT COUNT(*) AS num {live_from_where}")
            if partial_total:
                select_clauses.append(f"SELECT {partial_total} AS num")
        if not select_clauses:
            observed.outcome = "served"
            return 0
        unioned = " UNION ALL ".join(f"({c})" for c in select_clauses)
        final_sql = f"SELECT {metric_expressions.sum_or_zero('num')} FROM ({unioned})"
        result = self._execute_scalar_rollup_sql(
            final_sql,
            params,
            live_needs_params=live_needs_params,
            direct_live_tmp=direct_live_tmp,
            observed=observed,
            log_tag="count_rollup",
            fetchone_int=True,
        )
        if result is not None:
            observed.outcome = "served"
        return result

    _SLOW_URLS_ROLLUP_MIN_HOURS = 24

    def _eligible_rollup_window(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        has_filters: bool,
        require_top_asns: list[int] | None = None,
        min_hours: int | None = None,
        observation: ReaderCoverage | None = None,
    ) -> tuple[datetime, datetime] | None:
        return eligible_window(
            start_time,
            end_time,
            has_filters=has_filters,
            require_top_asns=require_top_asns,
            min_hours=min_hours if min_hours is not None else self._SLOW_URLS_ROLLUP_MIN_HOURS,
            observation=observation,
        )

    def try_slow_urls_from_rollup(
        self, start_time: str | None, end_time: str | None, *, has_filters: bool, min_requests: int, limit: int
    ) -> dict | None:
        if durable_serving_blocked(self.src):
            return None
        win = self._eligible_rollup_window(start_time, end_time, has_filters=has_filters)
        if win is None:
            return None
        st, et = win
        rollup_paths = collect_hour_paths_with_floor(self.src, st, et, SLOW_URLS_BUNDLE_FILENAME)
        if rollup_paths is None:
            return None
        paths_sql = quote_path_list(rollup_paths)
        sql = (
            f"SELECT url, "
            f"       {self._WEIGHTED_P50_P95_SELECT_SQL}, "
            f"       SUM(p99_us * requests) / NULLIF(SUM(requests), 0) AS p99_us_w "
            f"FROM read_parquet([{paths_sql}]) "
            f"GROUP BY url "
            f"HAVING SUM(requests) >= ? "
            f"ORDER BY p95_us_w DESC "
            f"LIMIT ?"
        )
        rows = self._exec_rollup(sql, [min_requests, limit], tag="slow_urls_rollup")
        if rows is None:
            return None
        out_rows = [self._shape_latency_row("url", r, p99_ms=_us_to_ms(r[4])) for r in rows]
        return {"has_data": len(out_rows) > 0, "rows": out_rows, "_approx": True}

    def _collect_rollup_paths(
        self,
        st: datetime,
        et: datetime,
        bundle_filename: str,
        *,
        skip_partial_start: bool = True,
        allow_empty_with_done: bool = False,
        observation: RollupPathCoverage | None = None,
    ) -> list[str] | None:
        return collect_day_prefer_paths(
            self.src,
            st,
            et,
            bundle_filename,
            skip_partial_start=skip_partial_start,
            allow_empty_with_done=allow_empty_with_done,
            observation=observation,
        )

    def _rollup_paths_sql(
        self,
        start_time: str | None,
        end_time: str | None,
        bundle_filename: str,
        *,
        has_filters: bool,
        require_top_asns: list[int] | None = None,
        min_hours: int | None = None,
        skip_partial_start: bool = True,
        allow_empty_with_done: bool = False,
    ) -> str | None:
        win = self._eligible_rollup_window(
            start_time, end_time, has_filters=has_filters, require_top_asns=require_top_asns, min_hours=min_hours
        )
        if win is None:
            return None
        rollup_paths = self._collect_rollup_paths(
            win[0],
            win[1],
            bundle_filename,
            skip_partial_start=skip_partial_start,
            allow_empty_with_done=allow_empty_with_done,
        )
        return quote_path_list(rollup_paths) if rollup_paths is not None else None

    def _exec_rollup(self, sql: str, params: list[Any] | None = None, *, tag: str, fetchone: bool = False) -> Any:
        try:
            cur = self.execute(sql, params) if params is not None else self.execute(sql)
            return cur.fetchone() if fetchone else cur.fetchall()
        except duckdb.InterruptException:
            raise
        except duckdb.Error as e:
            _logger.debug("[%s] read failed, falling back to raw: %s", tag, e)
            return None

    def try_origin_summary_from_rollup(
        self, start_time: str | None, end_time: str | None, *, has_filters: bool, actual_cols: set[str] | list[str]
    ) -> dict | None:
        paths_sql = self._rollup_paths_sql(
            start_time, end_time, ORIGIN_SUMMARY_BUNDLE_FILENAME, has_filters=has_filters
        )
        if paths_sql is None:
            return None
        sql = (
            f"SELECT "
            f"  CAST(SUM(requests) AS BIGINT)                                AS requests, "
            f"  CAST(SUM(total_misses) AS BIGINT)                            AS total_misses, "
            f"  CAST(SUM(total_passes) AS BIGINT)                            AS total_passes, "
            f"  SUM(lat_us_count)                                            AS lat_us_count, "
            f"  SUM(ottfb_p50_us * lat_us_count) / NULLIF(SUM(lat_us_count), 0) AS ottfb_p50_us, "
            f"  SUM(ottfb_p75_us * lat_us_count) / NULLIF(SUM(lat_us_count), 0) AS ottfb_p75_us, "
            f"  SUM(ottfb_p95_us * lat_us_count) / NULLIF(SUM(lat_us_count), 0) AS ottfb_p95_us, "
            f"  SUM(ottfb_p99_us * lat_us_count) / NULLIF(SUM(lat_us_count), 0) AS ottfb_p99_us, "
            f"  SUM(ottlb_p50_us * ottlb_count) / NULLIF(SUM(ottlb_count), 0)   AS ottlb_p50_us, "
            f"  SUM(ottlb_p95_us * ottlb_count) / NULLIF(SUM(ottlb_count), 0)   AS ottlb_p95_us, "
            f"  SUM(cdn_ovh_p50_us * cdn_ovh_count) / NULLIF(SUM(cdn_ovh_count), 0) AS cdn_ovh_p50_us, "
            f"  SUM(ost_5xx_count) * 1.0 / NULLIF(SUM(ost_total_count), 0)   AS origin_error_rate, "
            f"  SUM(obytes_p50 * obytes_count) / NULLIF(SUM(obytes_count), 0)  AS obytes_p50 "
            f"FROM read_parquet([{paths_sql}])"
        )
        row = self._exec_rollup(sql, tag="origin_summary_rollup", fetchone=True)
        if row is None or row[0] is None or row[0] == 0:
            return None
        cols_set = set(actual_cols)
        if row[4] is None:
            return None
        has_ottlb = "ottlb" in cols_set
        return {
            "has_data": True,
            "total_misses": int(row[1]) if row[1] is not None else None,
            "total_passes": int(row[2]) if row[2] is not None else None,
            "ottfb_p50_ms": _us_to_ms(row[4]),
            "ottfb_p75_ms": _us_to_ms(row[5]),
            "ottfb_p95_ms": _us_to_ms(row[6]),
            "ottfb_p99_ms": _us_to_ms(row[7]),
            "ottlb_p50_ms": _us_to_ms(row[8]) if has_ottlb else None,
            "ottlb_p95_ms": _us_to_ms(row[9]) if has_ottlb else None,
            "cdn_overhead_p50_ms": _us_to_ms(row[10]) if (has_ottlb and "elapsed" in cols_set) else None,
            "origin_error_rate": float(row[11]) if (row[11] is not None and "ost" in cols_set) else None,
            "obytes_p50": float(row[12]) if (row[12] is not None and "obytes" in cols_set) else None,
            "_approx": True,
        }

    _WEIGHTED_P50_P95_SELECT_SQL = (
        "CAST(SUM(requests) AS BIGINT) AS requests, "
        "       SUM(p50_us * requests) / NULLIF(SUM(requests), 0) AS p50_us_w, "
        "       SUM(p95_us * requests) / NULLIF(SUM(requests), 0) AS p95_us_w"
    )

    @staticmethod
    def _shape_latency_row(key_name: str, r: Any, **extra: Any) -> dict[str, Any]:
        return {
            key_name: r[0],
            "requests": int(r[1] or 0),
            "p50_ms": _us_to_ms(r[2]),
            "p95_ms": _us_to_ms(r[3]),
            **extra,
        }

    def try_origin_pop_latency_from_rollup(
        self, start_time: str | None, end_time: str | None, *, has_filters: bool, limit: int
    ) -> dict | None:
        paths_sql = self._rollup_paths_sql(start_time, end_time, ORIGIN_POP_BUNDLE_FILENAME, has_filters=has_filters)
        if paths_sql is None:
            return None
        sql = (
            f"SELECT pop, "
            f"       {self._WEIGHTED_P50_P95_SELECT_SQL} "
            f"FROM read_parquet([{paths_sql}]) "
            f"GROUP BY pop "
            f"ORDER BY p95_us_w DESC "
            f"LIMIT ?"
        )
        rows = self._exec_rollup(sql, [limit], tag="origin_pop_rollup")
        if not rows:
            return None
        valid_p95s = sorted(_us_to_ms(r[3]) for r in rows if r[3] is not None)  # type: ignore[type-var]
        median_p95 = valid_p95s[len(valid_p95s) // 2] if valid_p95s else 0
        out_rows = [
            self._shape_latency_row(
                "pop",
                r,
                elevated=(p95_ms := _us_to_ms(r[3])) is not None and median_p95 is not None and p95_ms > median_p95 * 2,
            )
            for r in rows
        ]
        return {
            "has_data": True,
            "requires_group_c": False,
            "median_p95_ms": median_p95,
            "rows": out_rows,
            "_approx": True,
        }

    def try_origin_ip_health_from_rollup(
        self, start_time: str | None, end_time: str | None, *, has_filters: bool, limit: int
    ) -> dict | None:
        paths_sql = self._rollup_paths_sql(start_time, end_time, ORIGIN_IP_BUNDLE_FILENAME, has_filters=has_filters)
        if paths_sql is None:
            return None
        sql = (
            f"SELECT oip, "
            f"       {self._WEIGHTED_P50_P95_SELECT_SQL}, "
            f"       ROUND(SUM(ost_5xx_count) * 100.0 / NULLIF(SUM(ost_total_count), 0), 1) AS error_pct "
            f"FROM read_parquet([{paths_sql}]) "
            f"GROUP BY oip "
            f"HAVING SUM(requests) >= 10 "
            f"ORDER BY error_pct DESC "
            f"LIMIT ?"
        )
        rows = self._exec_rollup(sql, [limit], tag="origin_ip_rollup")
        if not rows:
            return None
        out_rows = [
            self._shape_latency_row("oip", r, error_pct=float(r[4]) if r[4] is not None else None) for r in rows
        ]
        return {"has_data": len(out_rows) > 0, "rows": out_rows, "_approx": True}

    def try_origin_path_breakdown_from_rollup(
        self, start_time: str | None, end_time: str | None, *, has_filters: bool
    ) -> dict | None:
        paths_sql = self._rollup_paths_sql(start_time, end_time, ORIGIN_PATH_BUNDLE_FILENAME, has_filters=has_filters)
        if paths_sql is None:
            return None
        sql = f"SELECT edge,        {self._WEIGHTED_P50_P95_SELECT_SQL} FROM read_parquet([{paths_sql}]) GROUP BY edge"
        rows = self._exec_rollup(sql, tag="origin_path_rollup")
        if not rows:
            return None
        out_rows = [self._shape_latency_row("edge", r) for r in rows]
        return {
            "has_data": len(out_rows) > 0,
            "shielding_detected": any(r[0] is False for r in rows),
            "rows": out_rows,
            "_approx": True,
        }

    def try_origin_status_from_rollup(
        self, start_time: str | None, end_time: str | None, *, has_filters: bool
    ) -> dict | None:
        paths_sql = self._rollup_paths_sql(start_time, end_time, DAY_BUNDLE_FILENAME, has_filters=has_filters)
        if paths_sql is None:
            return None
        sql = (
            f"SELECT status, "
            f"       CAST(SUM(count) AS BIGINT) AS count, "
            f"       SUM(count) * 100.0 / SUM(SUM(count)) OVER () AS pct "
            f"FROM ("
            f"  SELECT CASE "
            f"           WHEN TRY_CAST(value AS INTEGER) BETWEEN 100 AND 599 "
            f"             THEN TRY_CAST(value AS INTEGER) "
            f"           ELSE -1 "
            f"         END AS status, "
            f"         count "
            f"  FROM read_parquet([{paths_sql}]) "
            f"  WHERE field = 'ost' AND value IS NOT NULL"
            f") "
            f"GROUP BY status "
            f"ORDER BY count DESC"
        )
        rows = self._exec_rollup(sql, tag="origin_status_rollup")
        if not rows:
            return None
        return {
            "has_data": True,
            "rows": [
                {"status": int(r[0]), "count": int(r[1]), "pct": float(r[2]) if r[2] is not None else None}
                for r in rows
            ],
        }

    def try_network_quality_from_rollup(
        self, start_time: str | None, end_time: str | None, *, has_filters: bool, region_country: str
    ) -> dict[str, Any] | None:
        win = self._eligible_rollup_window(
            start_time, end_time, has_filters=has_filters, require_top_asns=None, min_hours=24
        )
        if win is None:
            return None
        st, et = win

        def _run_dim(filename: str, filter_sql: str = "", filter_params: list | None = None) -> list[dict] | None:
            rollup_paths = self._collect_rollup_paths(st, et, filename)
            if not rollup_paths:
                return None
            paths_sql = quote_path_list(rollup_paths)
            sql = (
                f"SELECT dim_val as label, "
                f"       SUM(p50_us * requests) / NULLIF(SUM(requests), 0) / 1000.0 AS rtt_ms, "
                f"       SUM(requests) AS reqs "
                f"FROM read_parquet([{paths_sql}]) "
                f"WHERE 1=1 {filter_sql} "
                f"GROUP BY dim_val "
                f"ORDER BY reqs DESC "
                f"LIMIT 25"
            )
            rows = self.execute(sql, filter_params or []).fetchall()
            return [
                {"value": str(r[0]), "label": str(r[0]), "rtt_ms": round(float(r[1]), 2), "reqs": int(r[2])}
                for r in rows
                if r[1] is not None
            ]

        try:
            by_country = _run_dim(NETWORK_QUALITY_COUNTRY_FILENAME)
            by_asn = _run_dim(NETWORK_QUALITY_ASN_FILENAME) if by_country is not None else None
            by_pop = _run_dim(NETWORK_QUALITY_POP_FILENAME) if by_asn is not None else None
            by_region = (
                _run_dim(NETWORK_QUALITY_REGION_FILENAME, "AND country = ?", [region_country])
                if by_pop is not None
                else None
            )
            if by_country is None or by_asn is None or by_pop is None or by_region is None:
                return None
            return {
                "available": True,
                "by_country": by_country,
                "by_asn": by_asn,
                "by_pop": by_pop,
                "by_region": by_region,
                "region_country": region_country,
                "scatter": [],  # Scatter not supported in rollup; UI will just render empty or we handle it
                "countries": [c["value"] for c in by_country],
                "_approx": True,
            }
        except duckdb.InterruptException:
            raise
        except Exception as e:
            _logger.debug("[network_quality_rollup] read failed: %s", e)
            return None

    def try_network_rtt_from_rollup(
        self, start_time: str | None, end_time: str | None, *, top_asns: list[int], has_filters: bool
    ) -> dict[int, dict[str, float | None]] | None:
        paths_sql = self._rollup_paths_sql(
            start_time,
            end_time,
            NETWORK_RTT_BUNDLE_FILENAME,
            has_filters=has_filters,
            require_top_asns=top_asns,
            min_hours=24,
        )
        if paths_sql is None:
            return None
        asn_placeholders = ", ".join(["?"] * len(top_asns))
        sql = (
            f"SELECT asn, "
            f"  SUM(p95_us * rtt_count) / NULLIF(SUM(rtt_count), 0) AS p95_us, "
            f"  SUM(p99_us * rtt_count) / NULLIF(SUM(rtt_count), 0) AS p99_us "
            f"FROM read_parquet([{paths_sql}]) "
            f"WHERE asn IN ({asn_placeholders}) "
            f"GROUP BY asn"
        )
        rows = self._exec_rollup(sql, top_asns, tag="network_rtt_rollup")
        if rows is None:
            return None
        return {
            int(row[0]): {
                "p95_rtt_us": round(float(row[1]), 0) if row[1] is not None else None,
                "p99_rtt_us": round(float(row[2]), 0) if row[2] is not None else None,
            }
            for row in rows
        }

    def _rollup_window_and_paths_with_active_hour(
        self,
        start_time: str | None,
        end_time: str | None,
        bundle_filename: str,
        *,
        has_filters: bool,
        require_top_asns: list[int] | None = None,
        min_hours: int = 24,
    ) -> tuple[datetime, datetime, str, datetime, str | None] | None:
        from backend.core.rollups import _safe_table_for

        win = self._eligible_rollup_window(
            start_time, end_time, has_filters=has_filters, require_top_asns=require_top_asns, min_hours=min_hours
        )
        if win is None:
            return None
        st, et = win
        rollup_paths = self._collect_rollup_paths(st, et, bundle_filename)
        if rollup_paths is None:
            return None
        active_hour_start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        return st, et, quote_path_list(rollup_paths), active_hour_start, _safe_table_for(self.src)

    def try_network_speed_from_rollup(
        self, start_time: str | None, end_time: str | None, *, top_asns: list[int], has_filters: bool
    ) -> list[tuple[int, str, int]] | None:
        resolved = self._rollup_window_and_paths_with_active_hour(
            start_time, end_time, NETWORK_SPEED_BUNDLE_FILENAME, has_filters=has_filters, require_top_asns=top_asns
        )
        if resolved is None:
            return None
        _st, et, paths_sql, active_hour_start, base_table = resolved
        asn_placeholders = ", ".join(["?"] * len(top_asns))
        if et > active_hour_start:
            ah_iso, et_iso = active_hour_start.isoformat(), et.isoformat()
            sql = (
                f"SELECT asn, c_speed, CAST(SUM(cnt) AS BIGINT) AS cnt FROM ("
                f"  SELECT asn, c_speed, count AS cnt "
                f"  FROM read_parquet([{paths_sql}]) "
                f"  WHERE asn IN ({asn_placeholders}) "
                f"  UNION ALL "
                f"  SELECT asn, c_speed, CAST(COUNT(*) AS BIGINT) AS cnt "
                f"  FROM {base_table} "
                f"  WHERE timestamp >= TIMESTAMPTZ '{ah_iso}' AND timestamp < TIMESTAMPTZ '{et_iso}' "
                f"    AND asn IN ({asn_placeholders}) AND c_speed IS NOT NULL "
                f"  GROUP BY asn, c_speed"
                f") GROUP BY asn, c_speed "
                f"ORDER BY asn, cnt DESC"
            )
            top_asns = top_asns + top_asns
        else:
            sql = (
                f"SELECT asn, c_speed, CAST(SUM(count) AS BIGINT) AS cnt "
                f"FROM read_parquet([{paths_sql}]) "
                f"WHERE asn IN ({asn_placeholders}) "
                f"GROUP BY asn, c_speed "
                f"ORDER BY asn, cnt DESC"
            )
        rows = self._exec_rollup(sql, top_asns, tag="network_speed_rollup")
        return [(int(r[0]), str(r[1]), int(r[2])) for r in rows] if rows is not None else None

    def try_perf_latency_from_rollup(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        dimension: str,
        sort_by: str,
        has_filters: bool,
        min_requests: int,
        limit: int,
    ) -> dict | None:
        filename = PERF_TOP_URLS_BUNDLE_FILENAME if dimension == "url" else PERF_TOP_ASNS_BUNDLE_FILENAME
        sort_col = {"avg": "avg_us", "p50": "p50_us_w", "p95": "p95_us_w", "p99": "p99_us_w"}.get(sort_by, "p99_us_w")
        paths_sql = self._rollup_paths_sql(start_time, end_time, filename, has_filters=has_filters)
        if paths_sql is None:
            return None
        sql = (
            f"SELECT value, "
            f"       CAST(SUM(requests) AS BIGINT) AS requests, "
            f"       SUM(elapsed_sum) / NULLIF(SUM(elapsed_count), 0) AS avg_us, "
            f"       SUM(p50_us * requests) / NULLIF(SUM(requests), 0) AS p50_us_w, "
            f"       SUM(p95_us * requests) / NULLIF(SUM(requests), 0) AS p95_us_w, "
            f"       SUM(p99_us * requests) / NULLIF(SUM(requests), 0) AS p99_us_w "
            f"FROM read_parquet([{paths_sql}]) "
            f"GROUP BY value "
            f"HAVING SUM(requests) > ? "
            f"ORDER BY {sort_col} DESC "
            f"LIMIT ?"
        )
        rows = self._exec_rollup(sql, [min_requests, limit], tag="perf_latency_rollup")
        if rows is None:
            return None
        out_rows = [
            {
                "value": r[0],
                "requests": int(r[1] or 0),
                "avg_ms": _us_to_ms(r[2]),
                "p50_ms": _us_to_ms(r[3]),
                "p95_ms": _us_to_ms(r[4]),
                "p99_ms": _us_to_ms(r[5]),
            }
            for r in rows
        ]
        return {"rows": out_rows, "_approx": True}

    def _try_security_bucket_rollup(
        self, start_time: str | None, end_time: str | None, *, has_filters: bool, bundle_filename: str, log_tag: str
    ) -> list[dict] | None:
        paths_sql = self._rollup_paths_sql(start_time, end_time, bundle_filename, has_filters=has_filters)
        if paths_sql is None:
            return None
        sql = (
            f"SELECT bucket, CAST(SUM(count) AS BIGINT) AS c, MIN(min_val) AS mv "
            f"FROM read_parquet([{paths_sql}]) "
            f"GROUP BY bucket "
            f"ORDER BY mv"
        )
        rows = self._exec_rollup(sql, tag=log_tag)
        return [{"bucket": r[0], "count": int(r[1] or 0)} for r in rows] if rows else None

    def try_security_req_size_from_rollup(
        self, start_time: str | None, end_time: str | None, *, has_filters: bool
    ) -> list[dict] | None:
        return self._try_security_bucket_rollup(
            start_time,
            end_time,
            has_filters=has_filters,
            bundle_filename=SECURITY_REQ_SIZE_BUNDLE_FILENAME,
            log_tag="security_req_size_rollup",
        )

    def try_security_conn_reuse_from_rollup(
        self, start_time: str | None, end_time: str | None, *, has_filters: bool
    ) -> list[dict] | None:
        return self._try_security_bucket_rollup(
            start_time,
            end_time,
            has_filters=has_filters,
            bundle_filename=SECURITY_CONN_REUSE_BUNDLE_FILENAME,
            log_tag="security_conn_reuse_rollup",
        )

    _CONN_REUSE_TO_DASHBOARD_BUCKET: dict[str, str] = {
        "1 (None)": "1",
        "2-5": "2–5",
        "6-20": "6–20",
        "21-100": "21+",
        ">100": "21+",
    }
    _CONN_REQUESTS_BUCKET_ORDER: tuple[str, ...] = ("1", "2–5", "6–20", "21+")

    def _begin_observed_rollup_window(
        self,
        reader_name: Literal[
            "dashboard.conn_requests_histogram",
            "dashboard.ngwaf_bots",
            "dashboard.count",
            "dashboard.time_series",
            "dashboard.top_n",
            "dashboard.ua_bots",
        ],
        start_time: str | None,
        end_time: str | None,
        *,
        has_filters: bool,
        min_hours: int | None = 2,
    ) -> tuple[ReaderCoverage, tuple[datetime, datetime] | None]:
        observed = ReaderCoverage(reader=reader_name, minimum_window_hours=min_hours)
        self.reader_coverage.append(observed)
        win = self._eligible_rollup_window(
            start_time, end_time, has_filters=has_filters, min_hours=min_hours, observation=observed
        )
        if win is None:
            observed.outcome = "ineligible"
        return observed, win

    def _collect_observed_rollup_paths(
        self,
        observed: ReaderCoverage,
        st: datetime,
        et: datetime,
        bundle_name: Literal["security_conn_reuse", "all_fields", "ngwaf_bots"],
        filename: str,
    ) -> list[str] | None:
        attempt = RollupPathCoverage(bundle=bundle_name)
        observed.attempts.append(attempt)
        return self._collect_rollup_paths(st, et, filename, observation=attempt)

    def try_conn_requests_hist_from_rollup(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        has_filters: bool,
        actual_cols: list[str] | set[str] | None = None,
    ) -> dict | None:
        observed, win = self._begin_observed_rollup_window(
            "dashboard.conn_requests_histogram", start_time, end_time, has_filters=has_filters, min_hours=2
        )
        if win is None:
            return None
        st, et = win
        rollup_paths = self._collect_observed_rollup_paths(
            observed, st, et, "security_conn_reuse", SECURITY_CONN_REUSE_BUNDLE_FILENAME
        )
        use_all_fields = rollup_paths is None
        if use_all_fields:
            rollup_paths = self._collect_observed_rollup_paths(observed, st, et, "all_fields", DAY_BUNDLE_FILENAME)
            if rollup_paths is None:
                observed.outcome = "no_coverage"
                return None
        if rollup_paths is None:
            raise RuntimeError("rollup_paths is unexpectedly None")
        paths_sql = quote_path_list(rollup_paths)
        sql = (
            "SELECT value, CAST(SUM(count) AS BIGINT) AS c "
            f"FROM read_parquet([{paths_sql}], hive_partitioning=0) "
            "WHERE field = 'conn_requests' GROUP BY value"
            if use_all_fields
            else f"SELECT bucket, CAST(SUM(count) AS BIGINT) AS c FROM read_parquet([{paths_sql}]) GROUP BY bucket"
        )
        rows = self._exec_rollup(sql, tag="conn_requests_hist_rollup")
        if rows is None:
            observed.outcome = "read_failed"
            return None
        if not rows:
            observed.outcome = "empty_result"
            return None
        counts: dict[str, int] = {}
        for bucket, cnt in rows:
            if use_all_fields:
                try:
                    numeric = int(float(bucket))
                except (TypeError, ValueError):
                    continue
                bucket = (
                    "1" if numeric == 1 else "2–5" if 2 <= numeric <= 5 else "6–20" if 6 <= numeric <= 20 else "21+"
                )
            label = self._CONN_REUSE_TO_DASHBOARD_BUCKET.get(bucket, bucket)
            counts[label] = counts.get(label, 0) + int(cnt or 0)
        from backend.repositories import dashboard as _dash_sql

        for bucket, cnt in self._fetch_active_hour_live_rows(
            st,
            et,
            required_col="conn_requests",
            build_sql=lambda tmp: _dash_sql.CONN_REQUESTS_BUCKET.format(
                requests_metric=CANONICAL_METRICS["requests"], table_name=tmp, where_clause="1=1"
            ),
            log_tag="conn_requests_hist_rollup",
            actual_cols=actual_cols,
            observed=observed,
        ):
            counts[bucket] = counts.get(bucket, 0) + int(cnt or 0)
        known = [b for b in self._CONN_REQUESTS_BUCKET_ORDER if b in counts]
        extras = [b for b in counts if b not in self._CONN_REQUESTS_BUCKET_ORDER]
        top = [{"value": b, "count": counts[b]} for b in known + extras]
        observed.outcome = "served"
        return {"top": top, "total": sum(counts.values())}

    def try_security_coverage_from_rollup(
        self, start_time: str | None, end_time: str | None, *, has_filters: bool
    ) -> tuple[int, int] | None:
        paths_sql = self._rollup_paths_sql(start_time, end_time, SECURITY_COV_BUNDLE_FILENAME, has_filters=has_filters)
        if paths_sql is None:
            return None
        row = self._exec_rollup(
            f"SELECT SUM(total_rows), SUM(tls_populated) FROM read_parquet([{paths_sql}])",
            tag="security_coverage_rollup",
            fetchone=True,
        )
        if row is None or row[0] is None or int(row[0]) == 0:
            return None
        return int(row[0]), int(row[1] or 0)

    def try_security_proxies_from_rollup(
        self, start_time: str | None, end_time: str | None, *, has_filters: bool
    ) -> dict | None:
        from backend.utils.pop_utils import build_pop_lat_case_sql, build_pop_lon_case_sql

        win = self._eligible_rollup_window(start_time, end_time, has_filters=has_filters)
        if win is None:
            return None
        st, et = win
        quality_paths = self._collect_rollup_paths(
            st, et, SECURITY_PROXY_QUALITY_BUNDLE_FILENAME, allow_empty_with_done=True
        )
        flagged_paths = self._collect_rollup_paths(
            st, et, SECURITY_PROXY_FLAGGED_BUNDLE_FILENAME, allow_empty_with_done=True
        )
        if not quality_paths or flagged_paths is None:
            return None
        prefix = (
            f"WITH f AS MATERIALIZED ("
            f"  SELECT ip, asn, pop, rtt_min, tcp_rtt, lat, lon, country, city, SUM(cnt) AS cnt "
            f"  FROM read_parquet([{quote_path_list(flagged_paths)}]) "
            f"  WHERE ip IS NOT NULL AND ip != '' GROUP BY ALL"
            f"), pops AS MATERIALIZED ("
            f"  SELECT pop AS p_code, {build_pop_lat_case_sql()} AS pop_lat, {build_pop_lon_case_sql()} AS pop_lon "
            f"  FROM (SELECT DISTINCT pop FROM f)"
            f"), geo AS MATERIALIZED ("
            f"  SELECT g.pop AS g_pop, g.lat AS g_lat, g.lon AS g_lon, "
            f"         6371 * 2 * ASIN(SQRT(POWER(SIN(RADIANS(g.lat - p.pop_lat) / 2), 2) "
            f"           + COS(RADIANS(p.pop_lat)) * COS(RADIANS(g.lat)) * POWER(SIN(RADIANS(g.lon - p.pop_lon) / 2), 2))) AS distance_km "
            f"  FROM (SELECT DISTINCT pop, lat, lon FROM f WHERE lat IS NOT NULL AND lon IS NOT NULL) g "
            f"  LEFT JOIN pops p ON g.pop IS NOT DISTINCT FROM p.p_code"
            f"), m AS MATERIALIZED ("
            f"  SELECT f.ip, f.asn, f.pop, f.rtt_min, f.tcp_rtt, f.rtt_min / 1000.0 AS rtt_min_ms, "
            f"         f.tcp_rtt / 1000.0 AS tcp_rtt_ms, f.lat AS client_lat, f.lon AS client_lon, "
            f"         p.pop_lat AS pop_lat, p.pop_lon AS pop_lon, geo.distance_km AS distance_km, f.country, f.city, f.cnt "
            f"  FROM f LEFT JOIN pops p ON f.pop IS NOT DISTINCT FROM p.p_code "
            f"  LEFT JOIN geo ON f.pop IS NOT DISTINCT FROM geo.g_pop AND f.lat IS NOT DISTINCT FROM geo.g_lat AND f.lon IS NOT DISTINCT FROM geo.g_lon"
            f") "
            if flagged_paths
            else ""
        )
        dist_flag = "distance_km > (rtt_min_ms * 100.0 + 150.0)"
        tunnel_flag = "tcp_rtt > 0 AND (CAST(rtt_min_ms * 1000.0 AS DOUBLE) / tcp_rtt) < 0.1"
        try:
            quality_rows = self.execute(
                f"SELECT type, CAST(SUM(count) AS BIGINT) AS c FROM read_parquet([{quote_path_list(quality_paths)}]) GROUP BY type ORDER BY c DESC"
            ).fetchall()
            if not quality_rows:
                return None
            stats_row = (0, 0, 0)
            isp_rows: list[tuple] = []
            client_rows: list[tuple] = []
            if flagged_paths:
                combined_rows = self.execute(
                    prefix + f"SELECT 'stats' AS _sec, "
                    f"  COUNT(DISTINCT CASE WHEN {dist_flag} THEN ip WHEN {tunnel_flag} THEN ip ELSE NULL END) AS c1, "
                    f"  CAST({metric_expressions.sum_or_zero(f'CASE WHEN {dist_flag} THEN cnt WHEN {tunnel_flag} THEN cnt ELSE NULL END')} AS BIGINT) AS c2, "
                    f"  COUNT(DISTINCT CASE WHEN {dist_flag} THEN ip ELSE NULL END) AS c3, "
                    f"  NULL::VARCHAR AS ip, NULL::BIGINT AS asn, NULL::DOUBLE AS rtt_min_ms, "
                    f"  NULL::DOUBLE AS tcp_rtt_ms, NULL::DOUBLE AS distance_km, NULL::VARCHAR AS pop, "
                    f"  NULL::DOUBLE AS client_lat, NULL::DOUBLE AS client_lon, NULL::DOUBLE AS pop_lat, "
                    f"  NULL::DOUBLE AS pop_lon, NULL::BOOLEAN AS impossible_distance, "
                    f"  NULL::VARCHAR AS risk_level, NULL::VARCHAR AS country, NULL::VARCHAR AS city "
                    f"FROM m UNION ALL SELECT * FROM ("
                    f"  SELECT 'isp' AS _sec, NULL::BIGINT, CAST(SUM(cnt) AS BIGINT) AS c2, NULL::BIGINT, "
                    f"    NULL::VARCHAR, asn, NULL::DOUBLE, NULL::DOUBLE, NULL::DOUBLE, NULL::VARCHAR, "
                    f"    NULL::DOUBLE, NULL::DOUBLE, NULL::DOUBLE, NULL::DOUBLE, NULL::BOOLEAN, "
                    f"    NULL::VARCHAR, NULL::VARCHAR, NULL::VARCHAR FROM m "
                    f"  WHERE tcp_rtt > 0 AND (CAST(rtt_min AS DOUBLE) / tcp_rtt) < 0.1 GROUP BY asn ORDER BY c2 DESC LIMIT 10"
                    f") UNION ALL SELECT * FROM ("
                    f"  SELECT 'client' AS _sec, NULL::BIGINT, NULL::BIGINT, NULL::BIGINT, "
                    f"    ip, asn, ROUND(rtt_min_ms, 2), ROUND(tcp_rtt_ms, 2), ROUND(distance_km, 1), pop, "
                    f"    ROUND(client_lat, 4), ROUND(client_lon, 4), CAST(pop_lat AS DOUBLE), CAST(pop_lon AS DOUBLE), "
                    f"    CASE WHEN {dist_flag} THEN true ELSE false END AS impossible_distance, "
                    f"    CASE WHEN {dist_flag} THEN 'High' WHEN tcp_rtt_ms > 0 AND (rtt_min_ms / tcp_rtt_ms) < 0.1 THEN 'High' "
                    f"         WHEN tcp_rtt_ms > 0 AND (rtt_min_ms / tcp_rtt_ms) < 0.2 THEN 'Medium' ELSE 'Low' END AS risk_level, country, city "
                    f"  FROM m WHERE rtt_min_ms IS NOT NULL AND (({dist_flag}) OR (tcp_rtt_ms > 0 AND (rtt_min_ms / tcp_rtt_ms) < 0.2)) "
                    f"  ORDER BY CASE risk_level WHEN 'High' THEN 3 WHEN 'Medium' THEN 2 ELSE 1 END DESC, distance_km DESC LIMIT 100"
                    f")"
                ).fetchall()
                for r in combined_rows:
                    if r[0] == "stats":
                        stats_row = (r[1], r[2], r[3])
                    elif r[0] == "isp":
                        isp_rows.append((r[5], r[2]))
                    elif r[0] == "client":
                        client_rows.append(r[4:])
        except duckdb.InterruptException:
            raise
        except duckdb.Error as e:
            _logger.debug("[security_proxies_rollup] read failed, falling back: %s", e)
            return None
        _client_keys = (
            "ip",
            "asn",
            "rtt_min_ms",
            "tcp_rtt_ms",
            "distance_km",
            "pop",
            "client_lat",
            "client_lon",
            "pop_lat",
            "pop_lon",
            "impossible_distance",
            "risk_level",
            "country",
            "city",
        )
        return {
            "stats": {
                "active_proxies_count": stats_row[0] or 0,
                "total_requests_count": stats_row[1] or 0,
                "distance_mismatches_count": stats_row[2] or 0,
            },
            "quality_rows": [(r[0], r[1]) for r in quality_rows],
            "isps": [{"asn": r[0], "count": r[1]} for r in isp_rows],
            "clients": [dict(zip(_client_keys, r, strict=False)) for r in client_rows],
        }

    def try_security_bot_efficiency_from_rollup(
        self, start_time: str | None, end_time: str | None, *, has_filters: bool
    ) -> list[tuple[str, str, int, int]] | None:
        paths_sql = self._rollup_paths_sql(
            start_time, end_time, SECURITY_BOT_EFF_BUNDLE_FILENAME, has_filters=has_filters, allow_empty_with_done=True
        )
        if paths_sql is None:
            return None
        sql = (
            f"WITH g AS (SELECT kind, key, CAST(SUM(cnt) AS BIGINT) AS cnt, CAST(SUM(resp_bytes) AS BIGINT) AS resp_bytes "
            f"  FROM read_parquet([{paths_sql}]) GROUP BY kind, key), "
            f"r AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY kind ORDER BY resp_bytes DESC, cnt DESC) AS rn FROM g) "
            f"SELECT kind, key, cnt, resp_bytes FROM r WHERE kind != 'ua' OR rn <= {SECURITY_BOT_EFF_UA_TOP_K}"
        )
        rows = self._exec_rollup(sql, tag="security_bot_efficiency_rollup")
        if not rows or not any(r[0] == "site" for r in rows):
            return None
        return [(str(r[0]), str(r[1] or ""), int(r[2] or 0), int(r[3] or 0)) for r in rows]

    def try_perf_ttl_dist_from_rollup(
        self, start_time: str | None, end_time: str | None, *, has_filters: bool
    ) -> list[dict] | None:
        paths_sql = self._rollup_paths_sql(start_time, end_time, PERF_TTL_DIST_BUNDLE_FILENAME, has_filters=has_filters)
        if paths_sql is None:
            return None
        sql = f"SELECT bucket, CAST(SUM(count) AS BIGINT) AS count FROM read_parquet([{paths_sql}]) GROUP BY bucket ORDER BY MIN(min_ttl)"
        rows = self._exec_rollup(sql, tag="perf_ttl_dist_rollup")
        return [{"bucket": r[0], "count": int(r[1])} for r in rows] if rows is not None else None

    def try_perf_waterfall_from_rollup(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        has_filters: bool,
        actual_cols: set[str] | list[str] | None = None,
    ) -> dict[str, Any] | None:
        from backend.repositories.utils.response_shaping import shape_perf_scatter_row, shape_waterfall_avg

        win = self._eligible_rollup_window(start_time, end_time, has_filters=has_filters)
        if win is None:
            return None
        st, et = win
        ov_paths = self._collect_rollup_paths(st, et, OVERVIEW_BUNDLE_FILENAME)
        if not ov_paths:
            return None
        ov_sql = quote_path_list(ov_paths)
        desc_rows = self._exec_rollup(
            f"DESCRIBE SELECT * FROM read_parquet([{ov_sql}], union_by_name=true)",
            tag="perf_waterfall_schema",
        )
        if not desc_rows:
            return None
        ov_cols = {str(r[0]) for r in desc_rows}
        if "waterfall_count" in ov_cols:
            wf_row = self._exec_rollup(
                f"SELECT CAST({metric_expressions.sum_or_zero('waterfall_count')} AS BIGINT), "
                f"       SUM(edge_processing_sum) / NULLIF(SUM(waterfall_count), 0), "
                f"       SUM(origin_wait_sum) / NULLIF(SUM(waterfall_count), 0), "
                f"       SUM(origin_download_sum) / NULLIF(SUM(waterfall_count), 0), "
                f"       SUM(client_download_sum) / NULLIF(SUM(waterfall_count), 0) "
                f"FROM read_parquet([{ov_sql}], union_by_name=true)",
                tag="perf_waterfall_rollup",
                fetchone=True,
            )
            if wf_row is not None and wf_row[0] is not None and int(wf_row[0]) > 0:
                scatter: list[dict[str, Any]] = []
                if {"hit_ttfb_sum", "hit_ttfb_count", "miss_ttfb_sum", "miss_ttfb_count"}.issubset(ov_cols):
                    sc_rows = self._exec_rollup(
                        f"SELECT (hit_ttfb_sum / hit_ttfb_count) * 1000.0 AS origin_ms, "
                        f"       GREATEST(0.0, (hit_elapsed_sum / hit_elapsed_count) / 1000.0 - (hit_ttfb_sum / hit_ttfb_count) * 1000.0) AS edge_ms, "
                        f"       'HIT' AS cache "
                        f"FROM read_parquet([{ov_sql}], union_by_name=true) "
                        f"WHERE hit_ttfb_count > 0 AND hit_elapsed_count > 0 "
                        f"UNION ALL "
                        f"SELECT (miss_ttfb_sum / miss_ttfb_count) * 1000.0 AS origin_ms, "
                        f"       GREATEST(0.0, (miss_elapsed_sum / miss_elapsed_count) / 1000.0 - (miss_ttfb_sum / miss_ttfb_count) * 1000.0) AS edge_ms, "
                        f"       'MISS' AS cache "
                        f"FROM read_parquet([{ov_sql}], union_by_name=true) "
                        f"WHERE miss_ttfb_count > 0 AND miss_elapsed_count > 0 "
                        f"LIMIT 300",
                        tag="perf_scatter_rollup",
                    )
                    if sc_rows:
                        scatter = [shape_perf_scatter_row(r[0], r[1], r[2]) for r in sc_rows]
                return {
                    "waterfall": shape_waterfall_avg(
                        {
                            "edge_processing": wf_row[1],
                            "origin_wait": wf_row[2],
                            "origin_download": wf_row[3],
                            "client_download": wf_row[4],
                        }
                    ),
                    "scatter": scatter,
                    "_approx": True,
                }

        ts_paths = self._collect_rollup_paths(st, et, TIME_SERIES_BUNDLE_FILENAME)
        if not ts_paths:
            return None
        ts_sql = quote_path_list(ts_paths)
        ov_row = self._exec_rollup(
            f"SELECT CAST({metric_expressions.sum_or_zero('requests')} AS BIGINT), "
            f"       {metric_expressions.sum_or_zero('hit_elapsed_sum', zero='0.0')}, "
            f"       CAST({metric_expressions.sum_or_zero('hit_elapsed_count')} AS BIGINT), "
            f"       {metric_expressions.sum_or_zero('miss_elapsed_sum', zero='0.0')}, "
            f"       CAST({metric_expressions.sum_or_zero('miss_elapsed_count')} AS BIGINT) "
            f"FROM read_parquet([{ov_sql}], union_by_name=true)",
            tag="perf_waterfall_ov_fallback",
            fetchone=True,
        )
        if ov_row is None or ov_row[0] is None or int(ov_row[0]) <= 0:
            return None
        total_reqs = int(ov_row[0])
        hit_el_sum = float(ov_row[1] or 0.0)
        hit_el_cnt = int(ov_row[2] or 0)
        miss_el_sum = float(ov_row[3] or 0.0)
        miss_el_cnt = int(ov_row[4] or 0)
        total_el_cnt = hit_el_cnt + miss_el_cnt
        avg_elapsed_ms = ((hit_el_sum + miss_el_sum) / total_el_cnt) / 1000.0 if total_el_cnt > 0 else 0.0
        avg_hit_el_ms = (hit_el_sum / hit_el_cnt) / 1000.0 if hit_el_cnt > 0 else avg_elapsed_ms
        avg_miss_el_ms = (miss_el_sum / miss_el_cnt) / 1000.0 if miss_el_cnt > 0 else avg_elapsed_ms

        ts_row = self._exec_rollup(
            f"SELECT {metric_expressions.sum_or_zero('ttfb_sum', zero='0.0')}, CAST({metric_expressions.sum_or_zero('ttfb_count')} AS BIGINT) "
            f"FROM read_parquet([{ts_sql}], union_by_name=true)",
            tag="perf_waterfall_ts_fallback",
            fetchone=True,
        )
        if ts_row is None or ts_row[1] is None or int(ts_row[1]) <= 0:
            return None
        avg_ttfb_ms = (float(ts_row[0] or 0.0) / int(ts_row[1])) * 1000.0

        cols_set = set(actual_cols) if actual_cols is not None else set()
        avg_ottfb_ms = 0.0
        avg_ottlb_ms = 0.0
        if "ottfb" in cols_set or "ottlb" in cols_set:
            os_paths = self._collect_rollup_paths(st, et, ORIGIN_SUMMARY_BUNDLE_FILENAME)
            if os_paths:
                os_sql = quote_path_list(os_paths)
                os_row = self._exec_rollup(
                    f"SELECT SUM(ottfb_p50_us * lat_us_count) / NULLIF(SUM(lat_us_count), 0), "
                    f"       CAST({metric_expressions.sum_or_zero('lat_us_count')} AS BIGINT), "
                    f"       SUM(ottlb_p50_us * ottlb_count) / NULLIF(SUM(ottlb_count), 0), "
                    f"       CAST({metric_expressions.sum_or_zero('ottlb_count')} AS BIGINT) "
                    f"FROM read_parquet([{os_sql}], union_by_name=true)",
                    tag="perf_waterfall_os_fallback",
                    fetchone=True,
                )
                if os_row is not None:
                    if "ottfb" in cols_set and os_row[0] is not None and int(os_row[1] or 0) > 0:
                        avg_ottfb_ms = (float(os_row[0]) / 1000.0) * min(1.0, int(os_row[1]) / total_reqs)
                    if "ottlb" in cols_set and os_row[2] is not None and int(os_row[3] or 0) > 0:
                        avg_ottlb_ms = (float(os_row[2]) / 1000.0) * min(1.0, int(os_row[3]) / total_reqs)

        origin_wait = max(0.0, avg_ottfb_ms)
        origin_download = max(0.0, avg_ottlb_ms - avg_ottfb_ms)
        edge_processing = max(0.0, avg_ttfb_ms - avg_ottfb_ms)
        client_download = max(0.0, avg_elapsed_ms - max(avg_ottlb_ms, avg_ttfb_ms))

        scatter_fb: list[dict[str, Any]] = []
        sc_fb_rows = self._exec_rollup(
            f"SELECT (ttfb_sum / ttfb_count) * 1000.0 AS origin_ms, "
            f"       CASE WHEN {metric_expressions.coalesce('hits', '0')} * 2 >= {metric_expressions.coalesce('requests', '0')} THEN 'HIT' ELSE 'MISS' END AS cache "
            f"FROM read_parquet([{ts_sql}], union_by_name=true) "
            f"WHERE ttfb_count > 0 "
            f"ORDER BY bucket DESC LIMIT 300",
            tag="perf_scatter_ts_fallback",
        )
        if sc_fb_rows:
            for orig_ms_raw, cache_lbl in sc_fb_rows:
                orig_ms = float(orig_ms_raw or 0.0)
                base_el = avg_hit_el_ms if cache_lbl == "HIT" else avg_miss_el_ms
                edge_ms = max(0.0, base_el - orig_ms)
                scatter_fb.append(shape_perf_scatter_row(orig_ms, edge_ms, str(cache_lbl)))

        return {
            "waterfall": shape_waterfall_avg(
                {
                    "edge_processing": edge_processing,
                    "origin_wait": origin_wait,
                    "origin_download": origin_download,
                    "client_download": client_download,
                }
            ),
            "scatter": scatter_fb,
            "_approx": True,
        }

    def try_pop_health_from_rollup(
        self, start_time: str | None, end_time: str | None, *, has_filters: bool
    ) -> list[tuple] | None:
        resolved = self._rollup_window_and_paths_with_active_hour(
            start_time, end_time, POP_HEALTH_BUNDLE_FILENAME, has_filters=has_filters
        )
        if resolved is None:
            return None
        st, et, paths_sql, active_hour_start, base_table = resolved
        # Whole-hour bundles omit this leading slice; the merge below only restores the live tail.
        if st != st.replace(minute=0, second=0, microsecond=0):
            return None
        if et > active_hour_start:
            ah_iso, et_iso = active_hour_start.isoformat(), et.isoformat()
            query = (
                f"SELECT pop,  CAST(SUM(requests) AS BIGINT),  CAST(SUM(errors) AS BIGINT),  CAST(SUM(cache_hits) AS BIGINT),"
                f"  CAST(SUM(bandwidth_bytes) AS BIGINT),  CAST(SUM(p50_rtt_us * requests) / NULLIF(SUM(requests), 0) AS DOUBLE),"
                f"  CAST(SUM(p95_ttfb_ms * requests) / NULLIF(SUM(requests), 0) AS DOUBLE) FROM ("
                f"  SELECT pop, requests, errors, cache_hits, bandwidth_bytes, p50_rtt_us, p95_ttfb_ms "
                f"  FROM read_parquet([{paths_sql}])   WHERE pop IS NOT NULL AND pop != ''   UNION ALL "
                f"  SELECT pop, CAST(COUNT(*) AS BIGINT) AS requests, "
                f"         CAST(COUNT(*) FILTER (WHERE status >= 400 OR status = 0) AS BIGINT) AS errors, "
                f"         CAST(COUNT(*) FILTER (WHERE cache IN ('HIT', 'HIT-STALE')) AS BIGINT) AS cache_hits, "
                f"         CAST(SUM(resp_bytes) AS BIGINT) AS bandwidth_bytes, "
                f"         CAST(approx_quantile(tcp_rtt, 0.5) AS DOUBLE) AS p50_rtt_us, "
                f"         CAST(approx_quantile(ttfb, 0.95) * 1000.0 AS DOUBLE) AS p95_ttfb_ms   FROM {base_table} "
                f"  WHERE timestamp >= TIMESTAMPTZ '{ah_iso}' AND timestamp < TIMESTAMPTZ '{et_iso}'     AND pop IS NOT NULL AND pop != '' "
                f"  GROUP BY pop) GROUP BY pop"
            )
        else:
            query = (
                f"SELECT   pop,   CAST(SUM(requests) AS BIGINT),   CAST(SUM(errors) AS BIGINT),   CAST(SUM(cache_hits) AS BIGINT), "
                f"  CAST(SUM(bandwidth_bytes) AS BIGINT),   CAST(SUM(p50_rtt_us * requests) / NULLIF(SUM(requests), 0) AS DOUBLE), "
                f"  CAST(SUM(p95_ttfb_ms * requests) / NULLIF(SUM(requests), 0) AS DOUBLE) "
                f"FROM read_parquet([{paths_sql}]) WHERE pop IS NOT NULL AND pop != '' GROUP BY pop"
            )
        return self.execute(query).fetchall()
