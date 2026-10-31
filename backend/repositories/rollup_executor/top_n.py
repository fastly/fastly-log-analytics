from __future__ import annotations

import logging
import os
import time
import uuid as _uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Literal

import duckdb

from backend.core.duckdb import escape_sql_literal
from backend.core.log_fields import Group, in_group
from backend.core.rollups._common import (
    _VIRTUAL_FIELD_BACKING,
    DAY_BUNDLE_FILENAME,
    NGWAF_BOTS_BUNDLE_FILENAME,
    SECURITY_TOPIPS_BUNDLE_FILENAME,
    _build_virtual_field_copy_query,
    _hour_bundled_root,
    _ip_spread_root,
    _is_safe_ident,
    quote_path_list,
)
from backend.core.rollups.recompute import (
    MISSING_HOUR_HEAL_CAP as _MISSING_HOUR_HEAL_CAP,
)
from backend.core.rollups.recompute import (
    durable_serving_blocked,
)
from backend.core.rollups.recompute import (
    heal_where_clause as _heal_where_clause,
)
from backend.models.analytics import (
    ReaderCoverage,
    RollupPathCoverage,
    TopNRollupCoverage,
)
from backend.repositories.rollup_executor.exact_window import (
    HistoricalCoverageUnavailable,
    _drop_temp_table,
    range_sql,
    reference_exists,
    verified_boundary_table,
)
from backend.repositories.top_n_rollups import (  # noqa: F401
    _EDGE_PREFIXES,
    IpSpreadPlan,
    SchemaSeeds,
    TopNPaths,
    TopNPlan,
    TopNRow,
    TopNWindow,
    _bundled_day_files,
    _bundled_hour_files,
    _day_covered_by_any_field,
    _day_has_real_rollup,
    _has_real_parquet,
    _per_field_files,
    build_ip_spread_sql,
    build_rolled_sql,
    collect_field_totals,
    find_unrolled_hours,
    fold_rows,
    fold_sketches,
    live_topn_fields,
    plan_ip_spread_paths,
    plan_top_n_paths,
    qualify_limit_expr,
    spread_meta,
    truncate_top_n,
)
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry
from backend.utils.date_utils import parse_day_key_strict, parse_hour_key_strict, parse_iso_utc, safe_iso  # noqa: F401

_logger = logging.getLogger("backend.repositories._base")

_SchemaSeeds = SchemaSeeds
_TopNWindow = TopNWindow
_build_ip_spread_sql = build_ip_spread_sql
_build_rolled_sql = build_rolled_sql
_find_unrolled_hours = find_unrolled_hours
_fold_rows = fold_rows
_fold_sketches = fold_sketches
_live_topn_fields = live_topn_fields
_plan_ip_spread_paths = plan_ip_spread_paths
_plan_top_n_paths = plan_top_n_paths
_spread_meta = spread_meta
_truncate_top_n = truncate_top_n

_EMPTY_ROLLUP_WARN_TS: dict[tuple[str, str], float] = {}
_EMPTY_ROLLUP_WARN_INTERVAL_S = 300.0
_MISSING_HOUR_HEAL_WARN_TS: dict[str, float] = {}


def _should_emit_throttled_warn(
    table: dict[Any, float], key: Any, interval_s: float = _EMPTY_ROLLUP_WARN_INTERVAL_S, max_entries: int = 256
) -> bool:
    now, last = time.monotonic(), table.get(key)
    if last is not None and now - last < interval_s:
        return False
    if len(table) >= max_entries and key not in table:
        table.clear()
    table[key] = now
    return True


def _active_hour_overlap(st: datetime, et: datetime) -> tuple[datetime, datetime, datetime]:
    active_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    return active_dt, max(active_dt, st), min(active_dt + timedelta(hours=1), et)


_MTIME_SKEW_MARGIN_S = 300
_LIVE_TOPN_SKIP_FIELDS: frozenset[str] = frozenset(
    {
        "waf_req_id",  # per-request NGWAF id (near-unique; joined elsewhere, never a facet)
        "rid",  # request id (near-unique)
        "prid",  # parent request id (near-unique)
        "_source_file",  # internal FOS provenance column
        "timestamp",  # the time axis, not a top-N facet
        "elapsed",  # raw per-request latency (ms); shown via chart metrics, not a facet
        "ttfb",  # raw per-request time-to-first-byte
        "req_bytes",
        "req_header_bytes",
        "resp_bytes",  # raw per-request byte counts (served via the chart-metric temp)
        "lat",
        "lon",  # raw coordinates — geo facets are city/region/country/metro
        "waf_sig",  # raw col backing the virtual waf_sig_ind card (served via _exploded_top_n)
    }
    | {f.code for f in in_group(Group.METRICS)}
)


def _get_schema(con: duckdb.DuckDBPyConnection, src: dict) -> list[dict]:
    from backend.core.duckdb import get_schema

    return get_schema(con, src, stats=False)


def _quote_ident(col: str) -> str:
    return '"{}"'.format(col.replace('"', '""'))


CANONICAL_METRICS = {
    "hit_rate": "ROUND(COUNT(*) FILTER (WHERE {cache_col} IN ('HIT', 'HIT-STALE')) * 100.0 / NULLIF(COUNT(*), 0), 2)",
    "requests": "COUNT(*)",
    "avg_ttfb": "ROUND(AVG(ttfb) * 1000.0, 2)",
    "p95_ttfb": "ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ttfb) * 1000.0, 2)",
    "5xx_rate": "ROUND(COUNT(*) FILTER (WHERE status >= 500) * 100.0 / NULLIF(COUNT(*), 0), 2)",
    "4xx_rate": "ROUND(COUNT(*) FILTER (WHERE status >= 400 AND status < 500) * 100.0 / NULLIF(COUNT(*), 0), 2)",
    "avg_resp_bytes": "ROUND(AVG(resp_bytes), 2)",
    "total_resp_bytes": "SUM(resp_bytes)",
    "throughput": "ROUND(COALESCE(MEDIAN(CASE WHEN starts_with({cache_col}, 'HIT') AND {elapsed_col} > 0 THEN {resp_bytes_col} * 1e6 / NULLIF(CAST({elapsed_col} AS DOUBLE), 0) ELSE NULL END), 0), 2)",
    "req_size": "ROUND(COALESCE(MEDIAN(CAST({header_bytes_col} AS DOUBLE) + CAST({req_bytes_col} AS DOUBLE)), 0), 2)",
    "ttfb_ms": "ROUND(COALESCE(MEDIAN(CASE WHEN ttfb IS NOT NULL AND ttfb > 0 THEN ttfb * 1000.0 ELSE NULL END), 0), 2)",
}


class _TopNRollupMixin:
    if TYPE_CHECKING:
        con: duckdb.DuckDBPyConnection
        src: dict[str, Any]
        reader_coverage: list[ReaderCoverage]
        _shared_active_temps: list[tuple[frozenset[str], Any, Any, str]] | None
        _shared_active_seed_fields: tuple[str, ...]
        _last_active_direct_n_files: int

        def execute(self, q: str, p: list[Any] | None = None) -> Any: ...
        def get_schema_cols(self) -> list[str]: ...
        def create_filtered_temp_table(
            self,
            cols: list[str],
            actual_cols: list[str] | set[str],
            source_table: str,
            where_clause: str,
            params: list[Any] | None = None,
        ) -> str | None: ...
        def release_active_direct_temp(self, name: str) -> None: ...
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
        ) -> str | None: ...
        def _exec_rollup(
            self, sql: str, params: list[Any] | None = None, *, tag: str, fetchone: bool = False
        ) -> Any: ...
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
        ) -> tuple[ReaderCoverage, tuple[datetime, datetime] | None]: ...
        def _collect_observed_rollup_paths(
            self,
            observed: ReaderCoverage,
            st: datetime,
            et: datetime,
            bundle_name: Literal["security_conn_reuse", "all_fields", "ngwaf_bots"],
            filename: str,
        ) -> list[str] | None: ...

    def _create_active_hour_temp_direct(
        self, fields: list[str], actual_cols: list[str] | set[str], live_start: Any, live_end: Any
    ) -> str | None:
        from backend import config as svcconfig
        from backend.core.duckdb import _cache_dir

        try:
            cache_dir = _cache_dir(self.src)
        except Exception:
            return None
        buffer_dir = os.path.join(cache_dir, "buffer")
        active_hour_token = live_start.strftime("%Y-%m-%d-%H")
        hourly_dir = os.path.join(cache_dir, "data", f"timestamp_hour={active_hour_token}")
        requested: list[str] = ["timestamp"]
        for f in fields:
            if f in actual_cols and f not in requested:
                requested.append(f)
        requested_set = frozenset(requested)
        if self._shared_active_temps is not None:
            for cols_have, ws, we, name in self._shared_active_temps:
                if ws == live_start and we == live_end and requested_set <= cols_have:
                    self._last_active_direct_n_files = 0
                    return name
            for cols_have, ws, we, name in self._shared_active_temps:
                if ws <= live_start and we >= live_end and requested_set <= cols_have:
                    sub_name = f"t_active_direct_{_uuid.uuid4().hex}"
                    sub_where = (
                        f"timestamp >= TIMESTAMPTZ '{live_start.isoformat()}' "
                        f"AND timestamp < TIMESTAMPTZ '{live_end.isoformat()}'"
                    )
                    try:
                        self.con.execute(f'CREATE TEMP TABLE {sub_name} AS SELECT * FROM "{name}" WHERE {sub_where}')
                        self._shared_active_temps.append((cols_have, live_start, live_end, sub_name))
                        self._last_active_direct_n_files = 0
                        return sub_name
                    except Exception:
                        _logger.debug("QueryRunner: shared active temp slice failed", exc_info=True)
        projected: list[str] = list(requested)
        if self._shared_active_temps is not None and self._shared_active_seed_fields:
            for f in self._shared_active_seed_fields:
                if f in actual_cols and f not in projected:
                    projected.append(f)
        projected_set = frozenset(projected)
        mtime_floor = (live_start - timedelta(seconds=_MTIME_SKEW_MARGIN_S)).timestamp()
        try:
            from backend.core.storage._core import _tombstoned_parquet_paths

            tombstoned = _tombstoned_parquet_paths(buffer_dir)
        except Exception:
            tombstoned = set()

        def _list_parquets(d: str, *, prune_mtime: bool) -> list[str]:
            out: list[str] = []
            try:
                with os.scandir(d) as it:
                    for e in it:
                        if not e.name.endswith(".parquet") or e.name.startswith(".tmp_") or e.path in tombstoned:
                            continue
                        if prune_mtime:
                            try:
                                if e.stat().st_mtime < mtime_floor:
                                    continue
                            except OSError:
                                pass  # can't stat → keep (correctness over speed)
                        out.append(e.path)
            except OSError:
                pass
            return out

        durable_serving = svcconfig.is_durable_serving_mode(self.src)
        buffer_files = [] if durable_serving else _list_parquets(buffer_dir, prune_mtime=True)
        lake_table = None
        try:
            from backend.core.storage._core import ducklake_table_name

            cand = ducklake_table_name(self.src)
            if self.con.execute(
                "SELECT 1 FROM duckdb_tables() WHERE database_name = 'lake' AND table_name = ? LIMIT 1", [cand]
            ).fetchone():
                lake_table = cand
        except Exception:
            lake_table = None
        hourly_files = [] if durable_serving or lake_table else _list_parquets(hourly_dir, prune_mtime=False)
        self._last_active_direct_n_files = len(buffer_files) + len(hourly_files)
        if not buffer_files and not hourly_files and not lake_table:
            return "__empty__"
        where = (
            f"timestamp >= TIMESTAMPTZ '{live_start.isoformat()}' AND timestamp < TIMESTAMPTZ '{live_end.isoformat()}'"
        )

        def _create_sql(union_by_name: str) -> str:
            branches: list[str] = []
            if buffer_files:
                branches.append(
                    f"SELECT * FROM read_parquet([{quote_path_list(buffer_files)}], union_by_name={union_by_name}) WHERE {where}"
                )
            if lake_table:
                branches.append(f"SELECT * FROM lake.{_quote_ident(lake_table)} WHERE {where}")
            if hourly_files:
                branches.append(
                    f"SELECT * FROM read_parquet([{quote_path_list(hourly_files)}], union_by_name={union_by_name}) WHERE {where}"
                )
            union_sql = " UNION ALL BY NAME ".join(branches)
            desc = self.con.execute(f"SELECT * FROM ({union_sql}) LIMIT 0").description or []
            physical_cols = {d[0] for d in desc}
            from backend.core.storage._core import _finalize_view_sql

            target_table = "logs"  # QueryRunner active hour direct is only used for logs path right now
            finalized = _finalize_view_sql(
                union_sql,
                self.src,
                target_table=target_table,
                dynamic_schema_field_names=set(),
                existing_cols=physical_cols,
            )
            available_cols = physical_cols | {"timestamp_hour", "dt"}
            valid_projected = [c for c in projected if c in available_cols]
            valid_cols_sql = ", ".join(_quote_ident(c) for c in valid_projected) if valid_projected else "*"
            return f"SELECT {valid_cols_sql} FROM ({finalized})"

        temp_name = f"t_active_direct_{_uuid.uuid4().hex}"
        try:
            self.con.execute(f"CREATE TEMP TABLE {temp_name} AS " + _create_sql("false"))
        except duckdb.InterruptException:
            raise
        except Exception:
            _drop_temp_table(self.con, temp_name, "_create_active_hour_temp_direct")
            try:
                self.con.execute(f"CREATE TEMP TABLE {temp_name} AS " + _create_sql("true"))
            except duckdb.InterruptException:
                raise
            except Exception:
                _drop_temp_table(self.con, temp_name, "_create_active_hour_temp_direct")
                return None
        if self._shared_active_temps is not None:
            self._shared_active_temps.append((projected_set, live_start, live_end, temp_name))
        return temp_name

    def _partial_hour_adjusted_live_start(self, naive_live_start, active_hour_token: str, window_end):
        from backend.core.rollups.day_bundles import (
            read_partial_hour_all_fields,
            read_partial_hour_total,
            read_partial_hour_watermark,
        )

        watermark = read_partial_hour_watermark(self.src, active_hour_token)
        if watermark <= 0.0:
            return naive_live_start, [], 0
        watermark_dt = datetime.fromtimestamp(watermark, tz=UTC)
        active_hour_dt = parse_hour_key_strict(active_hour_token)
        if naive_live_start > active_hour_dt or window_end < watermark_dt:
            return naive_live_start, [], 0
        try:
            partial_rows = read_partial_hour_all_fields(self.src, active_hour_token, con=self.con)
            partial_total = read_partial_hour_total(self.src, active_hour_token, con=self.con)
        except Exception as e:
            _logger.warning(
                "[partial_hour] Failed reading partial hour for %s: %s; falling back to full live scan",
                active_hour_token,
                e,
            )
            return naive_live_start, [], 0
        if not partial_rows and partial_total == 0:
            return naive_live_start, [], 0
        return max(naive_live_start, watermark_dt), partial_rows, partial_total

    def execute_top_n_rollups(
        self,
        fields: list[str],
        start_time: str | None,
        end_time: str | None,
        limit: int = 10,
        per_field_limits: dict[str, int] | None = None,
        _phase_log: list[dict] | None = None,
        actual_cols: list[str] | None = None,
        schema_types: dict[str, str] | None = None,
        _coverage_reader: Literal["dashboard.top_n", "dashboard.ua_bots"] = "dashboard.top_n",
        exact_window: bool = False,
        field_totals: dict[str, int] | None = None,
    ) -> tuple[list[tuple[str, Any, int]], list[str]]:
        from backend.core.duckdb import _cache_dir
        from backend.core.rollups import _safe_table_for

        observed = ReaderCoverage(
            reader=_coverage_reader, maximum_window_days=None, eligible=False, outcome="ineligible"
        )
        if field_totals is not None:
            field_totals.clear()
        self.reader_coverage.append(observed)
        if durable_serving_blocked(self.src):
            observed.eligibility_reason = "durable_unready"
            return [], fields

        def _phase(name: str, ms: float) -> None:
            if _phase_log is not None:
                _phase_log.append({"section": f"top_n_rollups:{name}", "time_ms": round(ms, 2)})

        cache_dir = _cache_dir(self.src)
        safe_fields = [f for f in fields if _is_safe_ident(f)]
        if not safe_fields:
            observed.eligibility_reason = "unsafe_fields"
            return [], fields
        base_table = _safe_table_for(self.src)
        if not base_table:
            observed.eligibility_reason = "unsafe_service"
            return [], fields
        st_dt = parse_iso_utc(start_time) if start_time else None
        et_dt = parse_iso_utc(end_time) if end_time else None
        observed.window_start, observed.window_end = (
            (st_dt.isoformat() if st_dt else None),
            (et_dt.isoformat() if et_dt else None),
        )
        observed.eligible, observed.eligibility_reason, observed.outcome = True, "eligible", "unknown"
        observed.top_n_plan = TopNRollupCoverage()
        win = _TopNWindow.build(st_dt, et_dt, datetime.now(UTC))
        active_dt = win.active_dt
        plan_win = win
        if exact_window:
            closed_start = st_dt.replace(minute=0, second=0, microsecond=0) if st_dt else None
            if st_dt is not None and closed_start is not None and st_dt > closed_start:
                closed_start += timedelta(hours=1)
            closed_end = et_dt.replace(minute=0, second=0, microsecond=0) if et_dt else None
            plan_win = _TopNWindow.build(closed_start, closed_end, active_dt)
        _t_dir_enum = time.perf_counter()
        plan = _plan_top_n_paths(cache_dir, safe_fields, plan_win, observation=observed.top_n_plan)
        day_paths, hour_paths = plan.paths.day_paths, plan.paths.hour_paths
        bundled_hour_paths, bundled_day_paths = plan.paths.bundled_hour_paths, plan.paths.bundled_day_paths
        _phase("dir_enum", (time.perf_counter() - _t_dir_enum) * 1000)
        _phase("dir_enum:n_day_files", float(len(day_paths)))
        _phase("dir_enum:n_hour_files", float(len(hour_paths)))
        _phase("dir_enum:n_bundled_hour_files", float(len(bundled_hour_paths)))
        _phase("dir_enum:n_bundled_day_files", float(len(bundled_day_paths)))
        pre_merge_limit = max(limit * 10, 100) if limit > 0 else limit
        pre_merge_per_field_limits = (
            {k: max(v, pre_merge_limit) for k, v in per_field_limits.items()} if per_field_limits else None
        )

        def _full_window_raw() -> tuple[list[tuple[str, Any, int]], list[str]]:
            observed.outcome = "read_failed"
            if st_dt is not None and st_dt < active_dt - timedelta(hours=_MISSING_HOUR_HEAL_CAP):
                # Raw and aggregate retention are independent. Successful raw
                # SQL is not proof that it can replace old retained aggregates.
                raise HistoricalCoverageUnavailable(
                    "Historical top-N coverage unavailable: raw completeness is unverified "
                    "outside the 48-hour healing horizon"
                )
            if field_totals is not None:
                field_totals.clear()
            _t_raw = time.perf_counter()
            raw_res = self._top_n_exact_window_rows(
                win,
                [],
                safe_fields,
                base_table,
                pre_merge_limit,
                pre_merge_per_field_limits,
                _SchemaSeeds(actual_cols, schema_types),
                full_window=True,
                field_totals=field_totals,
            )
            _phase("raw_fallback", (time.perf_counter() - _t_raw) * 1000)
            top_results, _ = _truncate_top_n(_fold_rows(raw_res), fields, limit, per_field_limits)
            observed.outcome = "served"
            return top_results, fields

        old_boundary_hours = (
            plan.uncovered_old_boundary_hours(
                win,
                _MISSING_HOUR_HEAL_CAP,
                safe_fields,
                has_reference=lambda hour: reference_exists(self.src, hour),
            )
            if exact_window
            else []
        )
        verify_hours = set(old_boundary_hours)
        if exact_window:
            for bound in (st_dt, et_dt):
                if (
                    bound is not None
                    and bound < active_dt - timedelta(hours=_MISSING_HOUR_HEAL_CAP)
                    and bound != bound.replace(minute=0, second=0, microsecond=0)
                    and reference_exists(self.src, bound.strftime("%Y-%m-%d-%H"))
                ):
                    # A clipped old hour may have hourly/minute evidence even
                    # without a daily field bundle. Do not lose its interior
                    # raw while count/chart retain that same minute history.
                    verify_hours.add(bound.strftime("%Y-%m-%d-%H"))
        _t_rolled = time.perf_counter()
        rolled_q = _build_rolled_sql(
            plan.paths,
            safe_fields,
            pre_merge_limit,
            pre_merge_per_field_limits,
            **({"include_totals": True} if field_totals is not None else {}),
        )
        rolled_res: list = []
        if rolled_q is not None:
            try:
                rolled_res = self.execute(rolled_q).fetchall()
                if field_totals is not None:
                    rolled_res = collect_field_totals(rolled_res, field_totals)
                observed.top_n_plan.rolled_read = "used"
            except duckdb.InterruptException:
                raise
            except Exception as exc:
                observed.top_n_plan.rolled_read = "failed"
                if exact_window:
                    observed.outcome = "read_failed"
                    missing_selected_file = (
                        isinstance(exc, duckdb.IOException)
                        and any(marker in str(exc) for marker in ("No files found", "No such file or directory"))
                        and any(
                            path in str(exc)
                            for paths in (day_paths, hour_paths, bundled_hour_paths, bundled_day_paths)
                            for path in paths
                        )
                    )
                    if not missing_selected_file or st_dt is None or et_dt is None:
                        raise
                    # The stored plan is no longer usable. Do not retry its paths or
                    # merge a partial answer with live/heal slices: read the whole scope.
                    _phase("rolled_res", (time.perf_counter() - _t_rolled) * 1000)
                    _logger.info("[top_n_rollups] selected file missing; using exact full-window raw fallback")
                    return _full_window_raw()
                rolled_res = []
        _phase("rolled_res", (time.perf_counter() - _t_rolled) * 1000)
        schema = _SchemaSeeds(actual_cols, schema_types)
        if exact_window:
            _t_live = time.perf_counter()
            live_res, partial_rows = self._top_n_live_active_hour(
                win,
                safe_fields,
                base_table,
                pre_merge_limit,
                pre_merge_per_field_limits,
                schema,
                _phase,
                observation=observed,
                fail_on_error=True,
                compute_virtual=True,
                field_totals=field_totals,
            )
            _phase("live_active_hour", (time.perf_counter() - _t_live) * 1000)
            missing_hours = _find_unrolled_hours(
                plan, plan_win, safe_fields, _MISSING_HOUR_HEAL_CAP, observation=observed.top_n_plan
            )
            _t_raw = time.perf_counter()
            try:
                raw_res = self._top_n_exact_window_rows(
                    win,
                    sorted(set(missing_hours) | set(old_boundary_hours)),
                    safe_fields,
                    base_table,
                    pre_merge_limit,
                    pre_merge_per_field_limits,
                    schema,
                    active_virtual_covered=not bool(partial_rows),
                    field_totals=field_totals,
                    verification_hours=sorted(verify_hours),
                    verification_phase=_phase,
                )
            except Exception:
                observed.outcome = "read_failed"
                raise
            _phase("raw_boundaries", (time.perf_counter() - _t_raw) * 1000)
            if verify_hours:
                _phase("verified_old_boundary:n_hours", float(len(verify_hours)))
            if missing_hours:
                observed.closed_hour_heal = "used"
            real_fields = [field for field in safe_fields if field not in _VIRTUAL_FIELD_BACKING]
            if field_totals is not None:
                for field, values in _fold_rows([row for row in partial_rows if row[0] in real_fields]).items():
                    field_totals[field] = field_totals.get(field, 0) + sum(values.values())
            top_results, _ = _truncate_top_n(
                _fold_rows(rolled_res, raw_res, live_res, [row for row in partial_rows if row[0] in real_fields]),
                fields,
                limit,
                per_field_limits,
            )
            observed.outcome = "served"
            return top_results, fields
        _t_live = time.perf_counter()
        live_res, partial_rows = self._top_n_live_active_hour(
            win,
            safe_fields,
            base_table,
            pre_merge_limit,
            pre_merge_per_field_limits,
            schema,
            _phase,
            observation=observed,
            field_totals=field_totals,
        )
        _phase("live_active_hour", (time.perf_counter() - _t_live) * 1000)
        _t_heal = time.perf_counter()
        missing_hours = _find_unrolled_hours(
            plan, win, safe_fields, _MISSING_HOUR_HEAL_CAP, observation=observed.top_n_plan
        )
        heal_res = (
            self._top_n_heal_hours(
                missing_hours,
                win,
                safe_fields,
                base_table,
                pre_merge_limit,
                pre_merge_per_field_limits,
                schema,
                observation=observed,
                field_totals=field_totals,
            )
            if missing_hours
            else []
        )
        _phase("live_missing_hours", (time.perf_counter() - _t_heal) * 1000)
        _phase("live_missing_hours:n_hours", float(len(missing_hours)))
        _t_merge = time.perf_counter()
        by_field = _fold_rows(rolled_res, live_res, heal_res, partial_rows)
        if field_totals is not None:
            for field, values in _fold_rows(partial_rows).items():
                field_totals[field] = field_totals.get(field, 0) + sum(values.values())
        top_results, empty_fields = _truncate_top_n(by_field, fields, limit, per_field_limits)
        _service_key = self.src.get("name") or self.src.get("service_id") or "default"
        for field in empty_fields:
            if field in _LIVE_TOPN_SKIP_FIELDS:
                continue
            if _should_emit_throttled_warn(_EMPTY_ROLLUP_WARN_TS, (_service_key, field)):
                _logger.debug(
                    "[top_n_rollups] empty result for field=%r (service=%r, window=[%s,%s]) — "
                    "panel will render empty; check rollup backfill coverage if unexpected",
                    field,
                    _service_key,
                    start_time,
                    end_time,
                )
        _phase("merge_sort", (time.perf_counter() - _t_merge) * 1000)
        observed.outcome = "served"
        return top_results, fields

    def _top_n_exact_window_rows(
        self,
        win: _TopNWindow,
        missing_hours: list[str],
        fields: list[str],
        base_table: str,
        limit: int,
        per_field_limits: dict[str, int] | None,
        schema: _SchemaSeeds,
        *,
        active_virtual_covered: bool = True,
        full_window: bool = False,
        field_totals: dict[str, int] | None = None,
        verification_hours: list[str] | None = None,
        verification_phase: Callable[[str, float], None] | None = None,
    ) -> list[tuple]:
        verification_hours = verification_hours or []
        proof_hours = set(verification_hours)
        raw_hours = set(missing_hours) | proof_hours
        for bound in (win.st_dt, win.et_dt):
            if bound is not None:
                hour = bound.replace(minute=0, second=0, microsecond=0)
                if hour < win.active_dt and bound != hour:
                    raw_hours.add(hour.strftime("%Y-%m-%d-%H"))
        ranges: list[tuple[datetime, datetime, bool]] = []
        for h in sorted(raw_hours):
            h_start = parse_hour_key_strict(h)
            h_end = h_start + timedelta(hours=1)
            seg_start = max(h_start, win.st_dt) if win.st_dt is not None else h_start
            seg_end = min(h_end, win.et_dt) if win.et_dt is not None else h_end
            if seg_start >= seg_end:
                continue
            if ranges and ranges[-1][1] == seg_start and not ranges[-1][2]:
                ranges[-1] = (ranges[-1][0], seg_end, False)
            else:
                ranges.append((seg_start, seg_end, False))
        # Dashboard raw reads include the endpoint; hourly aggregates are half-open.
        if win.et_dt is not None and (win.st_dt is None or win.et_dt >= win.st_dt):
            if ranges and ranges[-1][1] == win.et_dt:
                ranges[-1] = (ranges[-1][0], win.et_dt, True)
            elif not any(start <= win.et_dt < end for start, end, _ in ranges):
                ranges.append((win.et_dt, win.et_dt, True))
        if full_window:
            if win.st_dt is None or win.et_dt is None:
                raise ValueError("Full-window top-N fallback requires both bounds")
            ranges = [(win.st_dt, win.et_dt, True)]
        if not ranges:
            return []
        schema.ensure(self.get_schema_cols, lambda: {c["name"]: c["type"] for c in _get_schema(self.con, self.src)})
        real_fields = [f for f in fields if f in schema.cols and f not in _VIRTUAL_FIELD_BACKING]
        virtual_pairs = [
            (f, b) for f in fields if (b := _VIRTUAL_FIELD_BACKING.get(f)) is not None and b in schema.cols
        ]
        if not real_fields and not virtual_pairs:
            return []
        proj_cols: list[str] = []
        if (virtual_pairs or proof_hours) and "timestamp" in schema.cols:
            proj_cols.append("timestamp")
        for f in real_fields:
            if f not in proj_cols:
                proj_cols.append(f)
        for _, b in virtual_pairs:
            if b not in proj_cols:
                proj_cols.append(b)
        cols_sql = ", ".join(_quote_ident(c) for c in proj_cols) if proj_cols else "*"
        full_proof_hours: list[str] = []
        proof_minutes: set[datetime] = set()
        for key in verification_hours:
            h_start = parse_hour_key_strict(key)
            h_end = h_start + timedelta(hours=1)
            lo = max(h_start, win.st_dt) if win.st_dt else h_start
            hi = min(h_end, win.et_dt) if win.et_dt else h_end
            if lo == h_start and hi == h_end:
                full_proof_hours.append(key)
                continue
            minute = lo.replace(second=0, microsecond=0)
            while minute < hi:
                proof_minutes.add(minute)
                minute += timedelta(minutes=1)
            if win.et_dt is not None and h_start <= win.et_dt < h_end:
                proof_minutes.add(win.et_dt.replace(second=0, microsecond=0))
        rows: list[tuple] = []
        with verified_boundary_table(
            self,
            base_table,
            cols_sql,
            ranges,
            full_proof_hours,
            proof_minutes=sorted(proof_minutes),
            label="top-N",
            phase=verification_phase,
        ) as tmp_name:
            if tmp_name is None:
                raise HistoricalCoverageUnavailable(
                    "Historical top-N coverage unavailable: exact boundary materialization is missing"
                )
            query_target = f'"{tmp_name}"'
            if proof_hours:
                query_target = f'(SELECT * FROM "{tmp_name}" WHERE {range_sql(ranges)})'
            n_rows = self.execute(f"SELECT COUNT(*) FROM {query_target}").fetchone()[0]
            if n_rows > 0:
                if real_fields:
                    batch_rows, _ = self.execute_top_n_batch(
                        real_fields,
                        query_target if proof_hours else tmp_name,
                        schema.cols,
                        schema.types,
                        limit=limit,
                        per_field_limits=per_field_limits,
                        **({"field_totals": field_totals} if field_totals is not None else {}),
                    )
                    rows.extend(batch_rows)
                if virtual_pairs:
                    v_queries = [
                        _build_virtual_field_copy_query(query_target, v_field, backing, "1=1")
                        for v_field, backing in virtual_pairs
                    ]
                    v_union = " UNION ALL ".join(f"({q})" for q in v_queries)
                    rows.extend(
                        self.execute(
                            f"SELECT field, value, SUM(count) AS c FROM ({v_union}) GROUP BY field, value"
                        ).fetchall()
                    )
        if not active_virtual_covered and virtual_pairs:
            live_start = max(win.active_dt, win.st_dt) if win.st_dt else win.active_dt
            live_end = min(win.active_dt_end, win.et_dt) if win.et_dt else win.active_dt_end
            if live_start < live_end:
                active_where = (
                    f"timestamp >= TIMESTAMPTZ '{live_start.isoformat()}' "
                    f"AND timestamp < TIMESTAMPTZ '{live_end.isoformat()}'"
                )
                v_queries = [
                    _build_virtual_field_copy_query(base_table, v_field, backing, active_where)
                    for v_field, backing in virtual_pairs
                ]
                v_union = " UNION ALL ".join(f"({q})" for q in v_queries)
                rows.extend(
                    self.execute(
                        f"SELECT field, value, SUM(count) AS c FROM ({v_union}) GROUP BY field, value"
                    ).fetchall()
                )
        if field_totals is not None:
            for field, values in _fold_rows([row for row in rows if row[0] in _VIRTUAL_FIELD_BACKING]).items():
                field_totals[field] = field_totals.get(field, 0) + sum(values.values())
        return rows

    def _top_n_live_active_hour(
        self,
        win: _TopNWindow,
        safe_fields: list[str],
        base_table: str,
        limit: int,
        per_field_limits: dict[str, int] | None,
        schema: _SchemaSeeds,
        _phase: Callable[[str, float], None],
        *,
        observation: ReaderCoverage | None = None,
        fail_on_error: bool = False,
        compute_virtual: bool = False,
        field_totals: dict[str, int] | None = None,
    ) -> tuple[list[tuple], list[tuple]]:
        st_dt, et_dt, active_dt = win.st_dt, win.et_dt, win.active_dt
        live_res: list[tuple] = []
        partial_rows: list[tuple] = []
        if observation is not None:
            observation.live_top_up = "outside_window"
        live_start = max(active_dt, st_dt) if st_dt else active_dt
        live_end = min(win.active_dt_end, et_dt) if et_dt else win.active_dt_end
        if (et_dt and et_dt <= active_dt) or (st_dt and st_dt >= win.active_dt_end):
            return live_res, partial_rows
        try:
            schema.ensure(self.get_schema_cols, lambda: {c["name"]: c["type"] for c in _get_schema(self.con, self.src)})
            actual_cols, schema_types = schema.cols, schema.types
            live_topn_fields = _live_topn_fields(safe_fields, _LIVE_TOPN_SKIP_FIELDS, self.src.get("log_fields"))
            virtual_pairs = (
                [(f, b) for f in safe_fields if (b := _VIRTUAL_FIELD_BACKING.get(f)) is not None and b in actual_cols]
                if compute_virtual
                else []
            )
            live_start, partial_rows, _partial_total = self._partial_hour_adjusted_live_start(
                live_start, win.active_str, live_end
            )
            live_where = f"timestamp >= '{live_start.isoformat()}' AND timestamp < '{live_end.isoformat()}'"
            if observation is not None:
                observation.live_start, observation.live_end = live_start.isoformat(), live_end.isoformat()
                observation.live_top_up = "unavailable"
            _t_lt = time.perf_counter()
            temp_proj_fields = list(live_topn_fields)
            if "waf_req_id" in actual_cols and "waf_req_id" not in temp_proj_fields:
                temp_proj_fields.append("waf_req_id")
            if virtual_pairs:
                if "timestamp" in actual_cols and "timestamp" not in temp_proj_fields:
                    temp_proj_fields.append("timestamp")
                for _, bcol in virtual_pairs:
                    if bcol not in temp_proj_fields:
                        temp_proj_fields.append(bcol)
            tmp_name = self._create_active_hour_temp_direct(temp_proj_fields, actual_cols, live_start, live_end)
            if tmp_name == "__empty__":
                tmp_name = None
                if observation is not None:
                    observation.live_top_up = "empty"
            elif tmp_name is None:
                tmp_name = self.create_filtered_temp_table(temp_proj_fields, actual_cols, base_table, live_where)
                if observation is not None and tmp_name:
                    observation.live_top_up = "view_fallback"
            elif observation is not None:
                observation.live_top_up = "unverified"
            _phase("live_active_hour:temp_create", (time.perf_counter() - _t_lt) * 1000)
            _phase("live_active_hour:n_files", float(self._last_active_direct_n_files))
            if tmp_name:
                try:
                    _n_live = 0
                    try:
                        _n_live = self.execute(f'SELECT COUNT(*) FROM "{tmp_name}"').fetchone()[0]
                        _phase("live_active_hour:n_rows", float(_n_live))
                    except Exception:
                        _n_live = 1
                    if observation is not None and _n_live == 0:
                        observation.live_top_up = "empty"
                    if _n_live > 0:
                        live_fields = [
                            f for f in live_topn_fields if f in actual_cols and f not in _VIRTUAL_FIELD_BACKING
                        ]
                        if observation is not None and not live_fields and not virtual_pairs:
                            observation.live_top_up = "missing_column"
                        if live_fields:
                            _t_lb = time.perf_counter()
                            live_res, _ = self.execute_top_n_batch(
                                live_fields,
                                tmp_name,
                                actual_cols,
                                schema_types,
                                limit=limit,
                                per_field_limits=per_field_limits,
                                **({"field_totals": field_totals} if field_totals is not None else {}),
                            )
                            if observation is not None and observation.live_top_up == "unverified":
                                observation.live_top_up = "used"
                            _phase("live_active_hour:batch", (time.perf_counter() - _t_lb) * 1000)
                        if virtual_pairs and not partial_rows:
                            v_queries = [
                                _build_virtual_field_copy_query(f'"{tmp_name}"', v_field, backing, "1=1")
                                for v_field, backing in virtual_pairs
                            ]
                            v_union = " UNION ALL ".join(f"({q})" for q in v_queries)
                            live_res.extend(
                                self.execute(
                                    f"SELECT field, value, SUM(count) AS c FROM ({v_union}) GROUP BY field, value"
                                ).fetchall()
                            )
                            if observation is not None and observation.live_top_up == "unverified":
                                observation.live_top_up = "used"
                finally:
                    self.release_active_direct_temp(tmp_name)
        except duckdb.InterruptException:
            raise
        except Exception:
            if observation is not None:
                observation.live_top_up = "failed"
            if fail_on_error:
                raise
            _logger.exception("[top_n_rollups] live active hour query failed")
        if field_totals is not None:
            for field, values in _fold_rows([row for row in live_res if row[0] in _VIRTUAL_FIELD_BACKING]).items():
                field_totals[field] = field_totals.get(field, 0) + sum(values.values())
        return live_res, partial_rows

    def _top_n_heal_hours(
        self,
        missing_hours: list[str],
        win: _TopNWindow,
        safe_fields: list[str],
        base_table: str,
        limit: int,
        per_field_limits: dict[str, int] | None,
        schema: _SchemaSeeds,
        *,
        observation: ReaderCoverage | None = None,
        field_totals: dict[str, int] | None = None,
    ) -> list[tuple]:
        heal_res: list[tuple] = []
        if observation is not None:
            observation.closed_hour_heal = "unavailable"
        _svc_key = self.src.get("name") or self.src.get("service_id") or "default"
        if _should_emit_throttled_warn(_MISSING_HOUR_HEAL_WARN_TS, _svc_key):
            _logger.warning(
                "[top_n_rollups] live-healing %d un-rolled closed hour(s) for service=%r "
                "(%s..%s) — expected when the writer is <1 heal tick behind; persistent "
                "occurrences mean the rollup_heal cron is failing (check cron_runs), and "
                "gaps older than %dh need the daily rollup_backfill cron (30-day deep pass) or an admin cron trigger",
                len(missing_hours),
                _svc_key,
                missing_hours[0],
                missing_hours[-1],
                _MISSING_HOUR_HEAL_CAP,
            )
        try:
            schema.ensure(self.get_schema_cols, lambda: {c["name"]: c["type"] for c in _get_schema(self.con, self.src)})
            actual_cols, schema_types = schema.cols, schema.types
            heal_topn_fields = _live_topn_fields(safe_fields, _LIVE_TOPN_SKIP_FIELDS, self.src.get("log_fields"))
            heal_where = _heal_where_clause(missing_hours, win.st_dt, win.et_dt)
            heal_tmp = self.create_filtered_temp_table(heal_topn_fields, actual_cols, base_table, heal_where)
            if heal_tmp:
                try:
                    heal_fields = [f for f in heal_topn_fields if f in actual_cols]
                    if heal_fields:
                        heal_res, _ = self.execute_top_n_batch(
                            heal_fields,
                            heal_tmp,
                            actual_cols,
                            schema_types,
                            limit=limit,
                            per_field_limits=per_field_limits,
                            **({"field_totals": field_totals} if field_totals is not None else {}),
                        )
                        if observation is not None:
                            observation.closed_hour_heal = "used"
                finally:
                    _drop_temp_table(self, heal_tmp, "execute_top_n_rollups")
        except duckdb.InterruptException:
            raise
        except Exception:
            if observation is not None:
                observation.closed_hour_heal = "failed"
            _logger.debug("[top_n_rollups] missing-hour live heal failed", exc_info=True)
        return heal_res

    def try_virtual_field_top_n_from_rollup(
        self,
        virtual_id: str,
        backing_col: str,
        start_time: str | None,
        end_time: str | None,
        *,
        has_filters: bool,
        actual_cols: list[str] | set[str] | None = None,
    ) -> dict[str, Any] | None:
        if has_filters or not start_time or not end_time:
            return None
        st, et = parse_iso_utc(start_time), parse_iso_utc(end_time)
        if st is None or et is None:
            return None
        paths = self._collect_rollup_paths(st, et, DAY_BUNDLE_FILENAME)
        if paths is None:
            return None
        paths_sql = quote_path_list(paths)
        _active_dt, live_start, live_end = _active_hour_overlap(st, et)
        rows: list[tuple] | None = None
        used_combined_live = False
        if live_start < live_end:
            cols = actual_cols if actual_cols is not None else self.get_schema_cols()
            if backing_col in cols:
                tmp = self._create_active_hour_temp_direct([backing_col], cols, live_start, live_end)
                if tmp == "__empty__":
                    return {"top": [], "total": 0}
                if tmp is None:
                    return None
                try:
                    rows = self._exec_rollup(
                        "SELECT value, CAST(SUM(c) AS BIGINT) AS total_c FROM ("
                        "SELECT value, CAST(SUM(count) AS BIGINT) AS c "
                        f"FROM read_parquet([{paths_sql}], hive_partitioning=0) "
                        "WHERE field = ? GROUP BY value "
                        "UNION ALL "
                        "SELECT trim(signal) AS value, COUNT(*) AS c "
                        f"FROM (SELECT unnest(string_split(\"{backing_col}\", ',')) AS signal "
                        f'FROM "{tmp}" WHERE "{backing_col}" IS NOT NULL AND "{backing_col}" != \'\') '
                        "WHERE trim(signal) != '' GROUP BY value"
                        ") GROUP BY value ORDER BY total_c DESC",
                        [virtual_id],
                        tag="virtual_field_top_n_rollup",
                    )
                    used_combined_live = True
                finally:
                    self.release_active_direct_temp(tmp)
        if not used_combined_live:
            rows = self._exec_rollup(
                "SELECT value, CAST(SUM(count) AS BIGINT) AS c "
                f"FROM read_parquet([{paths_sql}], hive_partitioning=0) "
                "WHERE field = ? GROUP BY value ORDER BY c DESC",
                [virtual_id],
                tag="virtual_field_top_n_rollup",
            )
        if rows is None:
            return None
        total = 0
        top: list[dict[str, Any]] = []
        for value, count in rows:
            c_val = int(count or 0)
            str_val = str(value)
            total += c_val
            if str_val != "__other__" and len(top) < 10:
                top.append({"value": str_val, "count": c_val})
        return {"top": top, "total": total}

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
    ) -> list[tuple]:
        _active_dt, live_start, live_end = _active_hour_overlap(st, et)
        if observed is not None:
            observed.live_top_up = "outside_window"
        if live_start >= live_end:
            return []
        if observed is not None:
            observed.live_start, observed.live_end = live_start.isoformat(), live_end.isoformat()
            observed.live_top_up = "missing_column"
        cols = actual_cols if actual_cols is not None else self.get_schema_cols()
        if required_col not in cols:
            return []
        tmp = self._create_active_hour_temp_direct([required_col], cols, live_start, live_end)
        if observed is not None:
            observed.live_top_up = "unavailable"
        if tmp == "__empty__":
            if observed is not None:
                observed.live_top_up = "empty"
            return []
        if tmp is None:
            return []
        try:
            rows = self.execute(build_sql(tmp)).fetchall()
            if observed is not None:
                observed.live_top_up = "used"
            return rows
        except duckdb.Error as e:
            if observed is not None:
                observed.live_top_up = "failed"
            _logger.debug("[%s] live top-up failed (skipped): %s", log_tag, e)
            return []
        finally:
            self.release_active_direct_temp(tmp)

    def _ngwaf_active_hour_live_rows(
        self,
        st: datetime,
        et: datetime,
        *,
        select_cols_sql: str,
        cache_table: str,
        group_by_sql: str,
        log_tag: str,
        observed: ReaderCoverage | None = None,
    ) -> list[tuple]:
        return self._fetch_active_hour_live_rows(
            st,
            et,
            required_col="waf_req_id",
            build_sql=lambda tmp: (
                f"SELECT {select_cols_sql}, CAST(COUNT(*) AS BIGINT) AS c "
                f'FROM "{tmp}" t '
                f"INNER JOIN temp.{cache_table} nb USING (waf_req_id) "
                f"WHERE nb.bot_name IS NOT NULL "
                f"GROUP BY {group_by_sql}"
            ),
            log_tag=log_tag,
            observed=observed,
        )

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
    ) -> list[tuple] | None:
        rows = self._exec_rollup(sql, tag=log_tag)
        if rows is None:
            if observed is not None:
                observed.outcome = "read_failed"
            return None
        return [
            *rows,
            *self._ngwaf_active_hour_live_rows(
                st,
                et,
                select_cols_sql=select_cols_sql,
                cache_table=cache_table,
                group_by_sql=group_by_sql,
                log_tag=log_tag,
                observed=observed,
            ),
        ]

    @staticmethod
    def _accumulate_row_counts(rows: list[tuple], key_fn: Callable[[tuple], Any]) -> dict[Any, int]:
        counts: dict[Any, int] = {}
        for row in rows:
            key = key_fn(row)
            counts[key] = counts.get(key, 0) + int(row[-1] or 0)
        return counts

    def try_ngwaf_top_bots_from_rollup(
        self, start_time: str | None, end_time: str | None, *, has_filters: bool, n: int
    ) -> list[dict] | None:
        observed, win = self._begin_observed_rollup_window(
            "dashboard.ngwaf_bots", start_time, end_time, has_filters=has_filters, min_hours=2
        )
        if win is None:
            return None
        st, et = win
        rollup_paths = self._collect_observed_rollup_paths(observed, st, et, "ngwaf_bots", NGWAF_BOTS_BUNDLE_FILENAME)
        if rollup_paths is None:
            observed.outcome = "no_coverage"
            return None
        paths_sql = quote_path_list(rollup_paths)
        sql = (
            f"SELECT bot_name, category, CAST(SUM(count) AS BIGINT) AS c "
            f"FROM read_parquet([{paths_sql}]) "
            f"GROUP BY bot_name, category"
        )
        combined = self._merge_ngwaf_rollup_and_live(
            st,
            et,
            sql=sql,
            select_cols_sql="nb.bot_name, nb.category",
            cache_table="ngwaf_top_ngwaf_bots",
            group_by_sql="1, 2",
            log_tag="ngwaf_bots_rollup",
            observed=observed,
        )
        if combined is None:
            return None
        counts = self._accumulate_row_counts(combined, lambda r: (r[0], r[1]))
        ranked = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)[: max(0, int(n))]
        observed.outcome = "served"
        return [{"name": bn, "category": cat, "request_count": c} for (bn, cat), c in ranked]

    def try_ngwaf_verified_bots_from_rollup(
        self, start_time: str | None, end_time: str | None, *, has_filters: bool
    ) -> list[dict] | None:
        import pyarrow.parquet as pq

        win = self._eligible_rollup_window(start_time, end_time, has_filters=has_filters)
        if win is None:
            return None
        st, et = win
        rollup_paths = self._collect_rollup_paths(st, et, NGWAF_BOTS_BUNDLE_FILENAME)
        if rollup_paths is None:
            return None
        try:
            for probe_path in (rollup_paths[0], rollup_paths[-1]):
                if "wellknown_bot_name" not in pq.read_schema(probe_path).names:
                    return None
        except Exception:
            return None
        paths_sql = quote_path_list(rollup_paths)
        sql = (
            f"SELECT bot_name, wellknown_bot_name, category, CAST(SUM(count) AS BIGINT) AS c "
            f"FROM read_parquet([{paths_sql}]) "
            f"GROUP BY bot_name, wellknown_bot_name, category"
        )
        combined = self._merge_ngwaf_rollup_and_live(
            st,
            et,
            sql=sql,
            select_cols_sql="nb.bot_name, nb.wellknown_bot_name, nb.category",
            cache_table="ngwaf_cache_ngwaf_bots",
            group_by_sql="1, 2, 3",
            log_tag="ngwaf_verified_bots_rollup",
        )
        if combined is None:
            return None
        counts = self._accumulate_row_counts(combined, lambda r: (r[0], r[1], r[2]))
        ranked = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)
        return [
            {"bot_name": bn, "wellknown_bot_name": wk, "category": cat, "request_count": c}
            for (bn, wk, cat), c in ranked
        ]

    def try_security_top_ips_from_rollup(
        self, start_time: str | None, end_time: str | None, *, has_filters: bool
    ) -> list[dict] | None:
        paths_sql = self._rollup_paths_sql(
            start_time, end_time, SECURITY_TOPIPS_BUNDLE_FILENAME, has_filters=has_filters
        )
        if paths_sql is None:
            return None
        sql = f"SELECT ip, CAST(MAX(max_header) AS BIGINT) AS mh FROM read_parquet([{paths_sql}]) GROUP BY ip ORDER BY mh DESC LIMIT 10"
        rows = self._exec_rollup(sql, tag="security_top_ips_rollup")
        return [{"ip": r[0], "max_header": int(r[1] or 0)} for r in rows] if rows else None

    def execute_ip_spread_rollups(
        self, fields: list[str], start_time: str | None, end_time: str | None, *, _phase_log: list[dict] | None = None
    ) -> tuple[dict[tuple[str, str], int], dict[str, dict]]:
        if durable_serving_blocked(self.src):
            return {}, {}

        def _phase(name: str, ms: float) -> None:
            if _phase_log is not None:
                _phase_log.append({"section": f"ip_spread_rollup:{name}", "time_ms": round(ms, 2)})

        safe_fields = [f for f in fields if _is_safe_ident(f)]
        if not safe_fields:
            return {}, {}
        win = _TopNWindow.build(
            parse_iso_utc(start_time) if start_time else None,
            parse_iso_utc(end_time) if end_time else None,
            datetime.now(UTC),
        )
        _t = time.perf_counter()
        plan = _plan_ip_spread_paths(_hour_bundled_root(self.src), _ip_spread_root(self.src), safe_fields, win)
        if plan is None:
            return {}, {}
        _phase("enumerate", (time.perf_counter() - _t) * 1000.0)
        if not plan:
            return {}, {}
        _t = time.perf_counter()
        try:
            rows = self.execute(_build_ip_spread_sql(plan, safe_fields)).fetchall()
        except duckdb.InterruptException:
            raise
        except duckdb.Error as e:
            _logger.debug("[ip_spread_rollup] read failed: %s", e)
            return {}, {}
        _phase("read", (time.perf_counter() - _t) * 1000.0)
        _t = time.perf_counter()
        merged, any_capped = _fold_sketches(rows)
        _phase("merge", (time.perf_counter() - _t) * 1000.0)
        return {k: int(v.count()) for k, v in merged.items()}, _spread_meta(safe_fields, plan, any_capped)

    def execute_top_n_batch(
        self,
        fields: list[str],
        table_name: str,
        actual_cols: list[str],
        schema_types: dict[str, str],
        limit: int = 10,
        per_field_limits: dict[str, int] | None = None,
        *,
        field_totals: dict[str, int] | None = None,
    ) -> tuple[list[tuple], list[str]]:
        INT_AGGREGATE_FIELDS = {"ttl", "age"}
        branches: list[str] = []
        field_order: list[str] = []
        _pfl = per_field_limits or {}
        try:
            actual_types = {
                r[0]: r[1] for r in self.con.execute(f"DESCRIBE SELECT * FROM {table_name} LIMIT 0").fetchall()
            }
        except Exception:
            actual_types = {}
        for field in fields:
            if not _is_safe_ident(field):
                continue
            col_type = actual_types.get(field) or schema_types.get(field, "VARCHAR")
            field_limit = int(_pfl.get(field, limit))
            field_lit = escape_sql_literal(field)
            if col_type == "VARCHAR":
                val_expr, where_pred = f'"{field}"', f'"{field}" IS NOT NULL AND "{field}" != \'\''
            elif field in INT_AGGREGATE_FIELDS:
                val_expr, where_pred = f'CAST(CAST(ROUND("{field}") AS INTEGER) AS VARCHAR)', f'"{field}" IS NOT NULL'
            else:
                val_expr, where_pred = f'CAST("{field}" AS VARCHAR)', f'"{field}" IS NOT NULL'
            branches.append(
                f"(SELECT '{field_lit}' AS field, {val_expr} AS value, count(*) AS c "
                + (", SUM(count(*)) OVER () AS field_total " if field_totals is not None else "")
                + f"FROM {table_name} WHERE {where_pred} GROUP BY 2 "
                f"ORDER BY c DESC LIMIT {field_limit})"
            )
            field_order.append(field)
        if not branches:
            return [], []
        rows = self.execute(
            "SELECT * FROM (\n            "
            + "\n            UNION ALL\n            ".join(branches)
            + "\n            ) ORDER BY field, c DESC"
        ).fetchall()
        if field_totals is not None:
            rows = collect_field_totals(rows, field_totals)
        return rows, field_order


_CacheRegistry.register("repositories._base._EMPTY_ROLLUP_WARN_TS", _EMPTY_ROLLUP_WARN_TS)
_CacheRegistry.register("repositories._base._MISSING_HOUR_HEAL_WARN_TS", _MISSING_HOUR_HEAL_WARN_TS)
