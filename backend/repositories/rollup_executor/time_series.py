from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import duckdb

from backend.core import log_fields as metric_expressions
from backend.core.rollups import recompute as rollup_coverage
from backend.core.rollups._common import (
    NETWORK_GEO_BUNDLE_FILENAME,
    NETWORK_HEATMAP_BUNDLE_FILENAME,
    NGWAF_BOTS_TS_BUNDLE_FILENAME,
    ORIGIN_LATENCY_TS_BUNDLE_FILENAME,
    VERIFIED_BOTS_TS_BUNDLE_FILENAME,
    quote_path_list,
)
from backend.core.rollups.recompute import (
    plan_hourly_read,
    plan_time_series_rollup_window,
)
from backend.models.analytics import (
    ReaderCoverage,
    RollupPathCoverage,
    ScalarRollupCoverage,
)
from backend.repositories.rollup_executor.exact_window import (
    HistoricalCoverageUnavailable,
    generation,
    has_subminute_bounds,
    plan_exact_minutes,
    range_sql,
    retained_window_requires_exact,
    verified_boundary_table,
)
from backend.repositories.rollup_executor.top_n import _active_hour_overlap
from backend.utils.date_utils import parse_iso_utc, safe_iso

_logger = logging.getLogger("backend.repositories._base")


class _TimeSeriesRollupMixin:
    if TYPE_CHECKING:
        con: duckdb.DuckDBPyConnection
        src: dict[str, Any]
        reader_coverage: list[ReaderCoverage]

        def execute(self, q: str, p: list[Any] | None = None) -> Any: ...
        def get_schema_cols(self) -> list[str]: ...
        def release_active_direct_temp(self, name: str) -> None: ...
        def _create_active_hour_temp_direct(
            self, fields: list[str], actual_cols: list[str] | set[str], live_start: Any, live_end: Any
        ) -> str | None: ...
        def _partial_hour_adjusted_live_start(
            self, naive_live_start: Any, active_hour_token: str, window_end: Any
        ) -> tuple[Any, list[Any], int]: ...
        def _eligible_rollup_window(
            self,
            start_time: str | None,
            end_time: str | None,
            *,
            has_filters: bool,
            require_top_asns: list[int] | None = None,
            min_hours: int | None = None,
            observation: ReaderCoverage | None = None,
        ) -> tuple[datetime, datetime] | None: ...
        def _collect_rollup_paths(
            self,
            st: datetime,
            et: datetime,
            bundle_filename: str,
            *,
            skip_partial_start: bool = True,
            allow_empty_with_done: bool = False,
            observation: RollupPathCoverage | None = None,
        ) -> list[str] | None: ...
        def _exec_rollup(
            self, sql: str, params: list[Any] | None = None, *, tag: str, fetchone: bool = False
        ) -> Any: ...
        def _rollup_window_and_paths_with_active_hour(
            self,
            start_time: str | None,
            end_time: str | None,
            bundle_filename: str,
            *,
            has_filters: bool,
            require_top_asns: list[int] | None = None,
            min_hours: int = 24,
        ) -> tuple[datetime, datetime, str, datetime, str | None] | None: ...
        def _merge_ngwaf_rollup_and_live(
            self,
            st: datetime,
            et: datetime,
            *,
            sql: str,
            select_cols_sql: str,
            cache_table: str,
            group_by_sql: str,
            log_tag: str,
            observed: ReaderCoverage | None = None,
        ) -> list[tuple] | None: ...
        @staticmethod
        def _accumulate_row_counts(rows: list[tuple], key_fn: Callable[[tuple], Any]) -> dict[Any, int]: ...

    _TS_ROLLUP_METRIC_PARTS: dict[str, dict[str, Any]] = {
        "requests": {
            "kind": "count",
            "num_rollup": "SUM(requests)",
            "den_rollup": "0",
            "num_live": "COUNT(*)",
            "den_live": "0",
            "live_cols": (),
        },
        "5xx": {
            "kind": "rate",
            "num_rollup": "SUM(status_5xx)",
            "den_rollup": "SUM(requests)",
            "num_live": "COUNT(*) FILTER (WHERE status >= 500)",
            "den_live": "COUNT(*)",
            "live_cols": ("status"),
        },
        "4xx": {
            "kind": "rate",
            "num_rollup": "SUM(status_4xx)",
            "den_rollup": "SUM(requests)",
            "num_live": "COUNT(*) FILTER (WHERE status BETWEEN 400 AND 499)",
            "den_live": "COUNT(*)",
            "live_cols": ("status"),
        },
        "hit_rate": {
            "kind": "rate",
            "num_rollup": "SUM(hits)",
            "den_rollup": "SUM(requests)",
            "num_live": "COUNT(*) FILTER (WHERE cache IN ('HIT', 'HIT-STALE'))",
            "den_live": "COUNT(*)",
            "live_cols": ("cache"),
        },
    }
    _TS_ROLLUP_INTERVALS: frozenset[str] = frozenset({"1 minute", "1 hour", "1 day"})
    _HOUR_START_TS_SQL_EXPR: str = (
        "TRY_CAST(CASE WHEN LENGTH(CAST(hour_start AS VARCHAR)) = 13 "
        "THEN CAST(hour_start AS VARCHAR) || ':00:00Z' "
        "ELSE CAST(hour_start AS VARCHAR) END AS TIMESTAMPTZ)"
    )

    def _exact_window_rollup_rows(
        self,
        start_time: str | None,
        end_time: str | None,
        table_name: str,
        where_clause: str,
        params: list,
        metric: str,
        interval: str | None,
        observed: ReaderCoverage,
        *,
        inclusive: bool,
    ) -> list[tuple] | None:
        if not start_time or not end_time or rollup_coverage.durable_serving_blocked(self.src):
            return None
        st, et = parse_iso_utc(start_time), parse_iso_utc(end_time)
        if st is None or et is None or et <= st or et - st > timedelta(days=366):
            return None
        observed.window_start, observed.window_end = st.isoformat(), et.isoformat()
        observed.eligible, observed.eligibility_reason, observed.outcome = True, "eligible", "unknown"
        observed.scalar_plan = ScalarRollupCoverage()
        parts = self._TS_ROLLUP_METRIC_PARTS[metric]
        try:
            plan = plan_exact_minutes(
                self.src,
                st,
                et,
                overview=metric == "requests" and interval != "1 minute",
                inclusive=inclusive,
            )
            selected = {path: generation(path) for path in [*plan.paths, *plan.overview_paths]}
            observed.scalar_plan.day_time_series_files = sum("/day=" in path for path in plan.paths)
            observed.scalar_plan.day_overview_files = len(plan.overview_paths)
            observed.scalar_plan.reason = "planned"
            observed.closed_hour_heal = "planned" if plan.recent_missing else "not_attempted"
            # CASE preserves the caller's raw predicates without filtering rows
            # out of the independent reference-count proof.
            live_cols = parts["live_cols"]
            cols = ["timestamp", *([live_cols] if isinstance(live_cols, str) else live_cols)]
            columns_sql = ", ".join(f'"{col}"' for col in cols)
            columns_sql += f", CASE WHEN ({where_clause}) THEN true ELSE false END AS _exact_selected"
            with verified_boundary_table(
                self,
                table_name,
                columns_sql,
                plan.raw_ranges,
                [],
                proof_minutes=plan.proof_minutes,
                empty_old_hours=plan.empty_old_hours,
                projection_params=params,
            ) as raw_table:
                clauses = []
                group = " GROUP BY 1" if interval else ""
                for paths, clock in ((plan.paths, "bucket"), (plan.overview_paths, self._HOUR_START_TS_SQL_EXPR)):
                    if not paths:
                        continue
                    bucket = f"time_bucket(INTERVAL '{interval}', {clock})" if interval else "NULL"
                    clauses.append(
                        f"SELECT {bucket} AS out_bucket, {parts['num_rollup']} AS num, "
                        f"{parts['den_rollup']} AS den FROM read_parquet([{quote_path_list(paths)}]) "
                        f"WHERE {clock} >= TIMESTAMPTZ '{plan.start.isoformat()}' "
                        f"AND {clock} < TIMESTAMPTZ '{plan.end.isoformat()}'{group}"
                    )
                if raw_table:
                    bucket = f"time_bucket(INTERVAL '{interval}', timestamp)" if interval else "NULL"
                    clauses.append(
                        f"SELECT {bucket} AS out_bucket, {parts['num_live']} AS num, {parts['den_live']} AS den "
                        f'FROM "{raw_table}" WHERE _exact_selected AND ({range_sql(plan.raw_ranges)}){group}'
                    )
                if not clauses:
                    rows = []
                else:
                    unioned = " UNION ALL ".join(f"({clause})" for clause in clauses)
                    value_expr = (
                        metric_expressions.sum_or_zero("num")
                        if parts["kind"] == "count"
                        else "ROUND(SUM(num) * 100.0 / NULLIF(SUM(den), 0), 2)"
                    )
                    rows = self.execute(
                        f"SELECT out_bucket, {value_expr}, {metric_expressions.sum_or_zero('den')} "
                        f"FROM ({unioned}) GROUP BY out_bucket ORDER BY out_bucket"
                    ).fetchall()
                if any(generation(path) != saved for path, saved in selected.items()):
                    raise HistoricalCoverageUnavailable(
                        "Historical analytics coverage unavailable: selected derivative changed"
                    )
            observed.closed_hour_heal = "used" if plan.recent_missing else "not_attempted"
            observed.outcome = "served"
            return rows
        except duckdb.InterruptException:
            raise
        except HistoricalCoverageUnavailable:
            observed.outcome = "read_failed"
            raise
        except Exception as exc:
            observed.outcome = "read_failed"
            raise HistoricalCoverageUnavailable(
                "Historical analytics coverage unavailable: exact retained read failed"
            ) from exc

    def _exact_time_series_from_rollup(
        self,
        chart_metric: str,
        interval: str,
        start_time: str | None,
        end_time: str | None,
        table_name: str,
        where_clause: str,
        params: list,
        observed: ReaderCoverage,
    ) -> list[dict] | None:
        if chart_metric not in self._TS_ROLLUP_METRIC_PARTS or interval not in self._TS_ROLLUP_INTERVALS:
            return None
        rows = self._exact_window_rollup_rows(
            start_time, end_time, table_name, where_clause, params, chart_metric, interval, observed, inclusive=True
        )
        if rows is None:
            return None
        return [
            {"time": safe_iso(bucket), "value": float(value) if value is not None else 0.0}
            for bucket, value, _ in rows
            if bucket is not None
        ]

    def _plan_scalar_rollup_window(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        can_use_overview: bool,
        require_minute_aligned: bool,
        observed: ReaderCoverage,
    ) -> tuple[datetime, datetime, Any, str, str, str, str | None] | None:
        if not start_time or not end_time:
            observed.eligibility_reason = "bounds_missing"
            return None
        st, et = parse_iso_utc(start_time), parse_iso_utc(end_time)
        if st is None or et is None or et <= st:
            observed.eligibility_reason = "invalid_window"
            return None
        observed.window_start, observed.window_end = st.isoformat(), et.isoformat()
        if require_minute_aligned and (st.second or st.microsecond or et.second or et.microsecond):
            observed.eligibility_reason = "unsupported_interval"
            return None
        if (et - st) > timedelta(days=366):
            observed.eligibility_reason = "window_too_long"
            return None
        observed.eligible, observed.eligibility_reason, observed.scalar_plan = True, "eligible", ScalarRollupCoverage()
        wplan = plan_time_series_rollup_window(
            self.src,
            st,
            et,
            can_use_overview=can_use_overview,
            coverage_mod=rollup_coverage,
            plan_hourly_read_fn=plan_hourly_read,
            observation=observed.scalar_plan,
        )
        if wplan is None:
            observed.outcome = "no_coverage"
            return None
        observed.outcome, observed.live_top_up = "unknown", "outside_window"
        heal_base_table: str | None = None
        if wplan.missing_hours:
            from backend.core.rollups import _safe_table_for

            heal_base_table = _safe_table_for(self.src)
            observed.closed_hour_heal = "planned" if heal_base_table else "unavailable"
        st_tz = st.astimezone(UTC).isoformat()
        st_hour_tz = st.replace(minute=0, second=0, microsecond=0).astimezone(UTC).isoformat()
        et_tz = et.astimezone(UTC).isoformat()
        return st, et, wplan, st_tz, st_hour_tz, et_tz, heal_base_table

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
    ) -> tuple[str | None, int, str | None, bool]:
        live_start, live_end = max(st, wplan.active_hour_dt), et
        if adjust_partial_hour:
            live_start, _partial_rows, partial_total = self._partial_hour_adjusted_live_start(
                live_start, wplan.active_hour_str, live_end
            )
        else:
            partial_total = 0
        live_st_tz, live_et_tz = live_start.astimezone(UTC).isoformat(), live_end.astimezone(UTC).isoformat()
        observed.live_start, observed.live_end = live_st_tz, live_et_tz
        direct_live_tmp: str | None = None
        live_needs_params = False
        if unfiltered_window:
            try:
                direct_live_tmp = get_direct_tmp(live_start, live_end)
            except Exception:
                direct_live_tmp = None
        if direct_live_tmp == "__empty__":
            live_source, live_where = None, "1=1"
        elif direct_live_tmp is not None:
            live_source, live_where = direct_live_tmp, "1=1"
        else:
            live_source, live_where, live_needs_params = table_name, where_clause, True
        observed.live_top_up = "empty" if live_source is None else "unverified"
        live_from_where = (
            f"FROM {live_source} "
            f"WHERE {live_where} "
            f"  AND timestamp >= TIMESTAMPTZ '{live_st_tz}' "
            f"  AND timestamp <  TIMESTAMPTZ '{live_et_tz}'"
            if live_source is not None
            else None
        )
        return live_from_where, partial_total, direct_live_tmp, live_needs_params

    def try_time_series_from_rollup(
        self,
        chart_metric: str,
        interval: str,
        start_time: str | None,
        end_time: str | None,
        table_name: str,
        where_clause: str,
        params: list,
        unfiltered_window: bool = False,
    ) -> list[dict] | None:
        observed = ReaderCoverage(reader="dashboard.time_series", eligible=False, outcome="ineligible")
        self.reader_coverage.append(observed)
        if chart_metric not in self._TS_ROLLUP_METRIC_PARTS:
            observed.eligibility_reason = "unsupported_metric"
            return None
        if interval not in self._TS_ROLLUP_INTERVALS:
            observed.eligibility_reason = "unsupported_interval"
            return None
        subminute = has_subminute_bounds(start_time, end_time)
        planned = (
            None
            if subminute and unfiltered_window
            else self._plan_scalar_rollup_window(
                start_time,
                end_time,
                can_use_overview=chart_metric == "requests" and interval in ("1 hour", "1 day"),
                require_minute_aligned=True,
                observed=observed,
            )
        )
        if not (subminute and unfiltered_window) and planned is None:
            # Reuse the canonical decline; the serving caller must select a
            # verified fallback, not repeat retained-artifact discovery here.
            return None
        if unfiltered_window and (
            subminute
            or (
                planned is not None
                and planned[2].missing_hours
                and retained_window_requires_exact(self.src, start_time, end_time, planned[2].rollup_paths)
            )
        ):
            return self._exact_time_series_from_rollup(
                chart_metric,
                interval,
                start_time,
                end_time,
                table_name,
                where_clause,
                params,
                observed,
            )
        if planned is None:
            return None
        st, et, wplan, st_tz, st_hour_tz, et_tz, heal_base_table = planned
        parts = self._TS_ROLLUP_METRIC_PARTS[chart_metric]
        select_clauses: list[str] = []
        if wplan.rollup_paths:
            paths_sql = quote_path_list(wplan.rollup_paths)
            select_clauses.append(
                f"SELECT time_bucket(INTERVAL '{interval}', bucket) AS out_bucket, "
                f"       {parts['num_rollup']} AS num, {parts['den_rollup']} AS den "
                f"FROM read_parquet([{paths_sql}]) "
                f"WHERE bucket >= TIMESTAMPTZ '{st_tz}' "
                f"  AND bucket < TIMESTAMPTZ '{et_tz}' "
                f"GROUP BY 1"
            )
        if wplan.day_ov_paths:
            ov_paths_sql = quote_path_list(wplan.day_ov_paths)
            hour_ts_expr = self._HOUR_START_TS_SQL_EXPR
            select_clauses.append(
                f"SELECT time_bucket(INTERVAL '{interval}', {hour_ts_expr}) AS out_bucket, "
                f"       {parts['num_rollup']} AS num, {parts['den_rollup']} AS den "
                f"FROM read_parquet([{ov_paths_sql}]) "
                f"WHERE {hour_ts_expr} >= TIMESTAMPTZ '{st_hour_tz}' "
                f"  AND {hour_ts_expr} < TIMESTAMPTZ '{et_tz}' "
                f"GROUP BY 1"
            )
        if heal_base_table:
            select_clauses.append(
                f"SELECT time_bucket(INTERVAL '{interval}', timestamp) AS out_bucket, "
                f"       {parts['num_live']} AS num, {parts['den_live']} AS den "
                f"FROM {heal_base_table} "
                f"WHERE {wplan.heal_where_sql} "
                f"GROUP BY 1"
            )
        direct_live_tmp: str | None = None
        live_needs_params = False
        if wplan.crosses_active:
            live_cols = parts["live_cols"]

            def _make_ts_direct_tmp(l_start: datetime, l_end: datetime) -> str | None:
                _cols: list[str] = self.get_schema_cols() if live_cols else []
                return (
                    self._create_active_hour_temp_direct(list(live_cols), _cols, l_start, l_end)
                    if all(c in _cols for c in live_cols)
                    else None
                )

            live_from_where, partial_total, direct_live_tmp, live_needs_params = self._resolve_scalar_live_source(
                st,
                et,
                wplan,
                table_name,
                where_clause,
                observed,
                adjust_partial_hour=(chart_metric == "requests"),
                unfiltered_window=unfiltered_window,
                get_direct_tmp=_make_ts_direct_tmp,
            )
            if live_from_where is not None:
                select_clauses.append(
                    f"SELECT time_bucket(INTERVAL '{interval}', timestamp) AS out_bucket, "
                    f"       {parts['num_live']} AS num, {parts['den_live']} AS den "
                    f"{live_from_where} "
                    f"GROUP BY 1"
                )
            if partial_total:
                partial_bucket_tz = wplan.active_hour_dt.astimezone(UTC).isoformat()
                select_clauses.append(
                    f"SELECT TIMESTAMPTZ '{partial_bucket_tz}' AS out_bucket, "
                    f"       {partial_total} AS num, {partial_total} AS den"
                )
        if not select_clauses:
            observed.outcome = "served"
            return []
        value_expr = (
            "CAST(SUM(num) AS BIGINT)"
            if parts["kind"] == "count"
            else "ROUND(SUM(num) * 100.0 / NULLIF(SUM(den), 0), 2)"
        )
        unioned = " UNION ALL ".join(f"({c})" for c in select_clauses)
        final_sql = (
            f"SELECT out_bucket, {value_expr} AS value "
            f"FROM ({unioned}) "
            f"WHERE out_bucket IS NOT NULL "
            f"GROUP BY out_bucket "
            f"ORDER BY out_bucket"
        )
        rows = self._execute_scalar_rollup_sql(
            final_sql,
            params,
            live_needs_params=live_needs_params,
            direct_live_tmp=direct_live_tmp,
            observed=observed,
            log_tag="time_series_rollup",
        )
        if rows is None:
            return None
        out = [
            {"time": safe_iso(r[0]), "value": float(r[1]) if r[1] is not None else 0.0}
            for r in rows
            if r[0] is not None
        ]
        observed.outcome = "served"
        return out

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
    ) -> Any:
        try:
            cur = self.execute(final_sql, params if live_needs_params else [])
            res = int(cur.fetchone()[0]) if fetchone_int else cur.fetchall()
            if observed.live_top_up == "unverified":
                observed.live_top_up = "view_fallback" if live_needs_params else "used"
            if observed.closed_hour_heal == "planned":
                observed.closed_hour_heal = "used"
            return res
        except duckdb.InterruptException:
            raise
        except Exception if fetchone_int else duckdb.Error as e:
            _logger.debug("[%s] read failed, falling back to raw: %s", log_tag, e)
            observed.outcome = "read_failed"
            if observed.closed_hour_heal == "planned":
                observed.closed_hour_heal = "failed"
            return None
        finally:
            if direct_live_tmp is not None:
                self.release_active_direct_temp(direct_live_tmp)

    def try_origin_latency_ts_from_rollup(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        has_filters: bool,
        bucket_minutes: float,
        metric: str,
        percentile: str,
        split_by_leg: bool,
        table_name: str,
        where_clause: str,
        params: list,
    ) -> dict | None:
        if split_by_leg or metric not in ("ttfb", "ttlb") or percentile not in ("p50", "p95", "p99"):
            return None
        try:
            bm = float(bucket_minutes)
        except (TypeError, ValueError):
            return None
        if bm < 1 or bm != int(bm):
            return None
        n_min = int(bm)
        win = self._eligible_rollup_window(start_time, end_time, has_filters=has_filters)
        if win is None:
            return None
        st, et = win
        plan = plan_hourly_read(self.src, st, et, ORIGIN_LATENCY_TS_BUNDLE_FILENAME)
        if plan is None:
            return None
        rollup_paths, crosses_active = plan.paths, plan.crosses_active
        cnt_col, pcol = f"{metric}_count", f"{metric}_{percentile}_us"
        st_tz, et_tz = st.astimezone(UTC).isoformat(), et.astimezone(UTC).isoformat()
        select_clauses: list[str] = []
        if rollup_paths:
            paths_sql = quote_path_list(rollup_paths)
            select_clauses.append(
                f"SELECT time_bucket(INTERVAL '{n_min} minutes', bucket_ts) AS out_bucket, "
                f"       SUM({pcol} * {cnt_col}) AS num, SUM({cnt_col}) AS den "
                f"FROM read_parquet([{paths_sql}]) "
                f"WHERE bucket_ts >= TIMESTAMPTZ '{st_tz}' "
                f"  AND bucket_ts <  TIMESTAMPTZ '{et_tz}' "
                f"GROUP BY 1"
            )
        if crosses_active:
            _active_hour_start, live_start, _ = _active_hour_overlap(st, et)
            live_st_tz, live_et_tz = live_start.astimezone(UTC).isoformat(), et.astimezone(UTC).isoformat()
            lat = self._origin_ts_live_lat_expr(metric, set(self.get_schema_cols()))
            if lat is None:
                return None
            pct_val = {"p50": 0.5, "p95": 0.95, "p99": 0.99}[percentile]
            per_min_pct = f"MEDIAN({lat})" if percentile == "p50" else f"APPROX_QUANTILE({lat}, {pct_val})"
            select_clauses.append(
                f"SELECT time_bucket(INTERVAL '{n_min} minutes', m) AS out_bucket, "
                f"       SUM(p_us * cnt) AS num, SUM(cnt) AS den "
                f"FROM ("
                f"  SELECT date_trunc('minute', timestamp) AS m, "
                f"         {per_min_pct} AS p_us, COUNT(*) AS cnt "
                f"  FROM {table_name} "
                f"  WHERE {where_clause} "
                f"    AND timestamp >= TIMESTAMPTZ '{live_st_tz}' "
                f"    AND timestamp <  TIMESTAMPTZ '{live_et_tz}' "
                f"    AND ({lat}) IS NOT NULL "
                f"  GROUP BY 1"
                f") GROUP BY 1"
            )
        if not select_clauses:
            return {"has_data": False, "series": [], "_approx": True}
        unioned = " UNION ALL ".join(f"({c})" for c in select_clauses)
        final_sql = (
            f"SELECT out_bucket, "
            f"       ROUND(SUM(num) / NULLIF(SUM(den), 0) / 1000.0, 3) AS value, "
            f"       CAST(SUM(den) AS BIGINT) AS miss_count "
            f"FROM ({unioned}) "
            f"WHERE out_bucket IS NOT NULL "
            f"GROUP BY out_bucket "
            f"ORDER BY out_bucket"
        )
        rows = self._exec_rollup(final_sql, params if crosses_active else [], tag="origin_latency_ts_rollup")
        if rows is None:
            return None
        series = [
            {
                "time": safe_iso(r[0]),
                "miss_count": int(r[2]) if r[2] is not None else 0,
                "value": float(r[1]) if r[1] is not None else None,
            }
            for r in rows
            if r[0] is not None
        ]
        return {"has_data": len(series) > 0, "series": series, "_approx": True}

    @staticmethod
    def _origin_ts_live_lat_expr(metric: str, cols: set[str]) -> str | None:
        if metric == "ttfb":
            return metric_expressions.origin_latency_us(cols, absent=None)
        return '"ottlb"' if "ottlb" in cols else None

    def try_network_heatmap_from_rollup(
        self, start_time: str | None, end_time: str | None, *, has_filters: bool, bucket_seconds: int = 3600
    ) -> list | None:
        if bucket_seconds > 3600:
            return None
        resolved = self._rollup_window_and_paths_with_active_hour(
            start_time, end_time, NETWORK_HEATMAP_BUNDLE_FILENAME, has_filters=has_filters
        )
        if resolved is None:
            return None
        st, et, paths_sql, active_hour_start, base_table = resolved
        st_iso, et_iso = st.isoformat(), et.isoformat()
        rtt_min_coal = metric_expressions.coalesce("rtt_min_p50_us", "rtt_p50_us")
        if et > active_hour_start:
            ah_iso = active_hour_start.isoformat()
            active_duration_secs = max(1, (et - active_hour_start).total_seconds())
            cols = set(self.get_schema_cols())
            has_rtt_min = "rtt_min" in cols
            rtt_base = (
                "CAST(approx_quantile(rtt_min, 0.5) AS DOUBLE)"
                if has_rtt_min
                else "CAST(approx_quantile(tcp_rtt, 0.5) AS DOUBLE)"
            )
            rtt_cong = (
                f"CAST(approx_quantile(tcp_rtt, 0.5) AS BIGINT) - CAST({metric_expressions.coalesce('approx_quantile(rtt_min, 0.5)', 'approx_quantile(tcp_rtt, 0.5)')} AS BIGINT)"
                if has_rtt_min
                else "0::BIGINT"
            )
            ploss_expr = "CAST(AVG(ploss) AS DOUBLE)" if "ploss" in cols else "0::DOUBLE"
            jitter_expr = "CAST(approx_quantile(rtt_var, 0.5) AS DOUBLE)" if "rtt_var" in cols else "0::DOUBLE"
            sql = (
                f"SELECT * FROM (  SELECT    asn,    hour_ts                                                             AS bucket_ts,"
                f"    resp_bytes_sum / 3600.0                                            AS throughput_bps,"
                f"    rtt_p50_us                                                         AS rtt_med_us,"
                f"    rtt_min_p50_us                                                     AS rtt_baseline_us,"
                f"    CAST(rtt_p50_us AS BIGINT) - CAST({rtt_min_coal} AS BIGINT) AS rtt_congestion_us,"
                f"    ploss_sum / NULLIF(ploss_count, 0)                                AS avg_ploss,"
                f"    rtt_var_p50_us                                                     AS jitter_us,"
                f"    errors * 100.0 / NULLIF(reqs, 0)                                  AS error_pct,"
                f"    CAST(reqs AS BIGINT)                                               AS reqs"
                f"   FROM read_parquet([{paths_sql}])   WHERE hour_ts >= TIMESTAMPTZ '{st_iso}' AND hour_ts < TIMESTAMPTZ '{et_iso}'"
                f"   UNION ALL    SELECT    asn,    TIMESTAMPTZ '{ah_iso}' AS bucket_ts,"
                f"    CAST(SUM(resp_bytes) AS DOUBLE) / {active_duration_secs} AS throughput_bps,"
                f"    CAST(approx_quantile(tcp_rtt, 0.5) AS DOUBLE) AS rtt_med_us,    {rtt_base} AS rtt_baseline_us,"
                f"    {rtt_cong} AS rtt_congestion_us,    {ploss_expr} AS avg_ploss,    {jitter_expr} AS jitter_us,"
                f"    CAST(COUNT(*) FILTER (WHERE status >= 400 OR status = 0) AS DOUBLE) * 100.0 / NULLIF(COUNT(*), 0) AS error_pct,"
                f"    CAST(COUNT(*) AS BIGINT) AS reqs   FROM {base_table} "
                f"   WHERE timestamp >= TIMESTAMPTZ '{ah_iso}' AND timestamp < TIMESTAMPTZ '{et_iso}'     AND asn IS NOT NULL   GROUP BY asn) ORDER BY reqs DESC"
            )
        else:
            sql = (
                f"SELECT  asn,  hour_ts                                                             AS bucket_ts,"
                f"  resp_bytes_sum / 3600.0                                            AS throughput_bps,"
                f"  rtt_p50_us                                                         AS rtt_med_us,"
                f"  rtt_min_p50_us                                                     AS rtt_baseline_us,"
                f"  CAST(rtt_p50_us AS BIGINT) - CAST({rtt_min_coal} AS BIGINT) AS rtt_congestion_us,"
                f"  ploss_sum / NULLIF(ploss_count, 0)                                AS avg_ploss,"
                f"  rtt_var_p50_us                                                     AS jitter_us,"
                f"  errors * 100.0 / NULLIF(reqs, 0)                                  AS error_pct,"
                f"  CAST(reqs AS BIGINT)                                               AS reqs"
                f" FROM read_parquet([{paths_sql}]) WHERE hour_ts >= TIMESTAMPTZ '{st_iso}' AND hour_ts < TIMESTAMPTZ '{et_iso}' ORDER BY reqs DESC"
            )
        rows = self._exec_rollup(sql, tag="network_heatmap_rollup")
        return list(rows) if rows else None

    def try_network_geo_from_rollup(
        self,
        start_time: str | None,
        end_time: str | None,
        *,
        map_asn: str = "all",
        has_filters: bool,
        bucket_seconds: int = 3600,
    ) -> tuple[list, list] | None:
        if int(bucket_seconds) > 3600 or map_asn != "all":
            return None
        resolved = self._rollup_window_and_paths_with_active_hour(
            start_time, end_time, NETWORK_GEO_BUNDLE_FILENAME, has_filters=has_filters
        )
        if resolved is None:
            return None
        st, et, paths_sql, active_hour_start, base_table = resolved
        st_iso, et_iso = st.isoformat(), et.isoformat()
        cols = set(self.get_schema_cols())
        if "country" not in cols:
            return None
        has_city, has_lat, has_metro = "city" in cols, ("lat" in cols and "lon" in cols), "metro" in cols
        city_col = "city" if has_city else "CAST('' AS VARCHAR)"
        lat_col = "CAST(lat AS DOUBLE)" if has_lat else "CAST(NULL AS DOUBLE)"
        lon_col = "CAST(lon AS DOUBLE)" if has_lat else "CAST(NULL AS DOUBLE)"
        metro_col = "metro" if has_metro else "CAST(NULL AS VARCHAR)"
        city_grp, lat_grp, lon_grp, metro_grp = (
            ("city" if has_city else "''"),
            ("lat" if has_lat else "NULL"),
            ("lon" if has_lat else "NULL"),
            ("metro" if has_metro else "NULL"),
        )
        ploss_live_expr = "CAST(AVG(ploss) AS DOUBLE)" if "ploss" in cols else "0::DOUBLE"
        if et > active_hour_start:
            ah_iso = active_hour_start.isoformat()
            map_sql = (
                f"SELECT * FROM (  SELECT    country, city, lat, lon, metro, hour_ts AS bucket_ts,"
                f"    rtt_sum / NULLIF(rtt_count, 0) AS rtt_med_us,    ploss_sum / NULLIF(ploss_count, 0) AS avg_ploss,"
                f"    errors * 100.0 / NULLIF(reqs, 0) AS error_pct,    CAST(reqs AS BIGINT) AS reqs"
                f"  FROM read_parquet([{paths_sql}])  WHERE hour_ts >= TIMESTAMPTZ '{st_iso}' AND hour_ts < TIMESTAMPTZ '{et_iso}'"
                f"  UNION ALL   SELECT    country,    {city_col} AS city,    {lat_col} AS lat,    {lon_col} AS lon,"
                f"    {metro_col} AS metro,    TIMESTAMPTZ '{ah_iso}' AS bucket_ts,"
                f"    CAST(approx_quantile(tcp_rtt, 0.5) AS DOUBLE) AS rtt_med_us,    {ploss_live_expr} AS avg_ploss,"
                f"    CAST(COUNT(*) FILTER (WHERE status >= 400 OR status = 0) AS DOUBLE) * 100.0 / NULLIF(COUNT(*), 0) AS error_pct,"
                f"    CAST(COUNT(*) AS BIGINT) AS reqs  FROM {base_table} "
                f"  WHERE timestamp >= TIMESTAMPTZ '{ah_iso}' AND timestamp < TIMESTAMPTZ '{et_iso}'    AND country IS NOT NULL AND country != ''"
                f"  GROUP BY country, {city_grp}, {lat_grp}, {lon_grp}, {metro_grp}) ORDER BY bucket_ts, reqs DESC LIMIT 5000"
            )
            metro_sql = (
                f"SELECT country, city, CAST('' AS VARCHAR) AS region, metro, "
                f"  CAST(SUM(rtt_med_us * reqs) / NULLIF(SUM(reqs), 0) AS DOUBLE) AS rtt_med_us,"
                f"  CAST(SUM(avg_ploss * reqs) / NULLIF(SUM(reqs), 0) AS DOUBLE) AS avg_ploss,"
                f"  CAST(SUM(error_pct * reqs) / NULLIF(SUM(reqs), 0) AS DOUBLE) AS error_pct,"
                f"  CAST(SUM(reqs) AS BIGINT) AS total_reqs FROM (  SELECT country, city, metro,"
                f"    SUM(rtt_sum) / NULLIF(SUM(rtt_count), 0) AS rtt_med_us,    SUM(ploss_sum) / NULLIF(SUM(ploss_count), 0) AS avg_ploss,"
                f"    SUM(errors) * 100.0 / NULLIF(SUM(reqs), 0) AS error_pct,    CAST(SUM(reqs) AS BIGINT) AS reqs"
                f"  FROM read_parquet([{paths_sql}])  WHERE hour_ts >= TIMESTAMPTZ '{st_iso}' AND hour_ts < TIMESTAMPTZ '{et_iso}'"
                f"  GROUP BY country, city, metro  UNION ALL   SELECT country, {city_col} AS city, {metro_col} AS metro,"
                f"    CAST(approx_quantile(tcp_rtt, 0.5) AS DOUBLE) AS rtt_med_us,    {ploss_live_expr} AS avg_ploss,"
                f"    CAST(COUNT(*) FILTER (WHERE status >= 400 OR status = 0) AS DOUBLE) * 100.0 / NULLIF(COUNT(*), 0) AS error_pct,"
                f"    CAST(COUNT(*) AS BIGINT) AS reqs  FROM {base_table} "
                f"  WHERE timestamp >= TIMESTAMPTZ '{ah_iso}' AND timestamp < TIMESTAMPTZ '{et_iso}'    AND country IS NOT NULL AND country != ''"
                f"  GROUP BY country, {city_grp}, {metro_grp}) GROUP BY country, city, metro ORDER BY total_reqs DESC LIMIT 100"
            )
        else:
            map_sql = (
                f"SELECT  country,  city,  lat,  lon,  metro,"
                f"  hour_ts                                           AS bucket_ts,"
                f"  rtt_sum / NULLIF(rtt_count, 0)                  AS rtt_med_us,"
                f"  ploss_sum / NULLIF(ploss_count, 0)              AS avg_ploss,"
                f"  errors * 100.0 / NULLIF(reqs, 0)                AS error_pct,"
                f"  CAST(reqs AS BIGINT)                             AS reqs"
                f" FROM read_parquet([{paths_sql}]) WHERE hour_ts >= TIMESTAMPTZ '{st_iso}' AND hour_ts < TIMESTAMPTZ '{et_iso}' ORDER BY hour_ts, reqs DESC LIMIT 5000"
            )
            metro_sql = (
                f"SELECT  country,  city,  CAST('' AS VARCHAR)                                   AS region,  metro,"
                f"  SUM(rtt_sum) / NULLIF(SUM(rtt_count), 0)             AS rtt_med_us,"
                f"  SUM(ploss_sum) / NULLIF(SUM(ploss_count), 0)         AS avg_ploss,"
                f"  SUM(errors) * 100.0 / NULLIF(SUM(reqs), 0)           AS error_pct,"
                f"  CAST(SUM(reqs) AS BIGINT)                             AS total_reqs"
                f" FROM read_parquet([{paths_sql}]) WHERE hour_ts >= TIMESTAMPTZ '{st_iso}' AND hour_ts < TIMESTAMPTZ '{et_iso}'"
                f" GROUP BY country, city, metro ORDER BY total_reqs DESC LIMIT 100"
            )
        try:
            map_rows = self.execute(map_sql).fetchall()
            metro_rows = self.execute(metro_sql).fetchall()
        except duckdb.InterruptException:
            raise
        except duckdb.Error as e:
            _logger.debug("[network_geo_rollup] read failed, falling back: %s", e)
            return None
        return (list(map_rows), list(metro_rows)) if (map_rows or metro_rows) else None

    def _minute_ts_rollup_window_and_paths(
        self,
        start_time: str | None,
        end_time: str | None,
        bucket_seconds: int,
        bundle_filename: str,
        *,
        has_filters: bool,
    ) -> tuple[int, datetime, datetime, str] | None:
        if has_filters or not start_time or not end_time:
            return None
        try:
            n = int(bucket_seconds)
        except (TypeError, ValueError):
            return None
        if n < 60 or n % 60 != 0:
            return None
        win = self._eligible_rollup_window(start_time, end_time, has_filters=has_filters)
        if win is None:
            return None
        st, et = win
        rollup_paths = self._collect_rollup_paths(st, et, bundle_filename, skip_partial_start=False)
        return (n, st, et, quote_path_list(rollup_paths)) if rollup_paths is not None else None

    def try_verified_bots_ts_from_rollup(
        self, start_time: str | None, end_time: str | None, *, temp_table: str, bucket_seconds: int, has_filters: bool
    ) -> list[tuple] | None:
        resolved = self._minute_ts_rollup_window_and_paths(
            start_time, end_time, bucket_seconds, VERIFIED_BOTS_TS_BUNDLE_FILENAME, has_filters=has_filters
        )
        if resolved is None:
            return None
        n, st, et, paths_sql = resolved
        active_iso = datetime.now(UTC).replace(minute=0, second=0, microsecond=0).isoformat()
        st_iso, et_iso = st.isoformat(), et.isoformat()
        sql = (
            f"SELECT bucket, bot_type, CAST(SUM(count) AS BIGINT) AS count FROM ("
            f"  SELECT time_bucket(INTERVAL '{n} seconds', bucket_ts) AS bucket, "
            f"         bot_type, SUM(count) AS count "
            f"  FROM read_parquet([{paths_sql}]) "
            f"  WHERE bucket_ts >= TIMESTAMPTZ '{st_iso}' AND bucket_ts < TIMESTAMPTZ '{et_iso}' "
            f"  GROUP BY 1, 2 "
            f"  UNION ALL "
            f"  SELECT time_bucket(INTERVAL '{n} seconds', timestamp) AS bucket, "
            f"         replace(tag, 'VERIFIED-BOT.', '') AS bot_type, COUNT(*) AS count "
            f"  FROM ("
            f"    SELECT timestamp, unnest(string_split(waf_sig, ',')) AS tag "
            f"    FROM {temp_table} "
            f"    WHERE waf_sig IS NOT NULL AND waf_sig ILIKE '%VERIFIED-BOT.%' "
            f"      AND timestamp >= TIMESTAMPTZ '{active_iso}' AND timestamp < TIMESTAMPTZ '{et_iso}'"
            f"  ) sub "
            f"  WHERE tag LIKE 'VERIFIED-BOT.%' "
            f"  GROUP BY 1, 2"
            f") GROUP BY bucket, bot_type ORDER BY bucket, bot_type"
        )
        rows = self._exec_rollup(sql, tag="verified_bots_ts_rollup")
        return [(r[0], str(r[1]), int(r[2])) for r in rows] if rows is not None else None

    def try_ngwaf_verified_bots_ts_from_rollup(
        self, start_time: str | None, end_time: str | None, *, bucket_seconds: int, has_filters: bool
    ) -> list[tuple] | None:
        resolved = self._minute_ts_rollup_window_and_paths(
            start_time, end_time, bucket_seconds, NGWAF_BOTS_TS_BUNDLE_FILENAME, has_filters=has_filters
        )
        if resolved is None:
            return None
        n, st, et, paths_sql = resolved
        st_iso, et_iso = st.isoformat(), et.isoformat()
        sql = (
            f"SELECT time_bucket(INTERVAL '{n} seconds', bucket_ts) AS bucket, "
            f"       bot_name, "
            f"       CAST(SUM(count) AS BIGINT) AS c "
            f"FROM read_parquet([{paths_sql}]) "
            f"WHERE bucket_ts >= TIMESTAMPTZ '{st_iso}' AND bucket_ts < TIMESTAMPTZ '{et_iso}' "
            f"GROUP BY 1, 2"
        )
        combined = self._merge_ngwaf_rollup_and_live(
            st,
            et,
            sql=sql,
            select_cols_sql=f"time_bucket(INTERVAL '{n} seconds', t.timestamp) AS bucket, nb.bot_name",
            cache_table="ngwaf_cache_ngwaf_bots",
            group_by_sql="1, 2",
            log_tag="ngwaf_verified_bots_ts_rollup",
        )
        if combined is None:
            return None
        counts = self._accumulate_row_counts(combined, lambda r: (r[0], str(r[1])))
        return [(b, bn, c) for (b, bn), c in sorted(counts.items())]
