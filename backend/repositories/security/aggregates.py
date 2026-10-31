from __future__ import annotations

import logging
import os
import time as _time
from collections.abc import Callable
from typing import Any

import duckdb

from backend import config as svcconfig
from backend.core.duckdb import escape_sql_literal
from backend.core.log_fields import coalesce
from backend.models.common import FiltersDict
from backend.repositories._base import (
    QueryRunner,
    SectionTimer,
    _drop_temp_table,
    _safe_table,
    ensure_ngwaf_bots_materialized,
    safe_iso,
    time_bucket_select,
)
from backend.repositories.security import sql as SQL
from backend.repositories.security.watchdog import (
    _empty_proxies_payload,
    _finish_proxies,
    _has_rollup_coverage,
    _ipv6_per_hour_from_rollups,
    _proxy_dist_from_rollups,
    _window_eligible_for_rollup,
    get_security_proxies,
    get_top_bots,
    summarize_bot_efficiency_rows,
    summarize_wellknown_bot_rows,
)
from backend.repositories.utils.filters import build_where_clause
from backend.repositories.utils.response_cache import (
    ResponseMemo,
    digest_cache_key,
    serialize_filters_for_key,
)
from backend.repositories.utils.response_shaping import (
    SECURITY_SECTION_DEFAULTS,
    empty_security_aggregates,
    shape_security_fingerprint_row,
    shape_security_ngwaf_bot_row,
    shape_security_top_ip_header_row,
)
from backend.utils.bounded_cache import BoundedTTLCache, CacheRegistry

_logger = logging.getLogger(__name__)
_SECURITY_CACHE_TTL = float(os.getenv("SECURITY_CACHE_TTL_SECONDS", "30"))
_memo = ResponseMemo(ttl=_SECURITY_CACHE_TTL, maxsize=128, prewarmer=False)
_response_cache: BoundedTTLCache = _memo.cache
CacheRegistry.register("security_aggregates_memo", _memo.cache)
CacheRegistry.register("security._response_cache", _response_cache)

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
    "referer",
    "resp_bytes",
    "waf_req_id",
    "pop",
    "rtt_min",
    "tcp_rtt",
    "lat",
    "lon",
    "asn",
    "country",
    "city",
)
_SECTION_DEFAULTS = SECURITY_SECTION_DEFAULTS


def _security_cache_bound(ts: str | None) -> str | None:
    if not ts:
        return None
    try:
        from backend.utils.date_utils import parse_iso_utc

        parsed = parse_iso_utc(str(ts))
        return parsed.isoformat() if parsed is not None else str(ts).strip()
    except (ValueError, TypeError):
        return str(ts).strip()


def _security_memo_key(
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict | None,
    bucket_seconds: int = 300,
    sections: set[str] | None = None,
    mask_ips: bool = False,
) -> str:
    payload = {
        "ep": "security_aggregates",
        "s": _security_cache_bound(start_time),
        "e": _security_cache_bound(end_time),
        "f": serialize_filters_for_key(filters),
        "b": int(bucket_seconds),
        "sec": sorted(sections) if sections is not None else None,
        "m": bool(mask_ips),
        "cd": str(src.get("_cache_dir_override", "")),
    }
    return digest_cache_key(payload, src)


def peek_security_aggregates(
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict | None,
    bucket_seconds: int = 300,
    sections: set[str] | None = None,
    mask_ips: bool = False,
) -> dict | None:
    if _SECURITY_CACHE_TTL <= 0 or start_time is None or end_time is None:
        return None
    key = _security_memo_key(src, start_time, end_time, filters, bucket_seconds, sections, mask_ips)
    cached = _memo.get_with_telemetry(key)
    return dict(cached) if cached is not None else None


def get_security_aggregates(
    con: duckdb.DuckDBPyConnection | Callable[[], duckdb.DuckDBPyConnection],
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    bucket_seconds: int = 300,
    sections: set[str] | None = None,
    mask_ips: bool = False,
) -> dict:
    memo_key: str | None = None
    if _SECURITY_CACHE_TTL > 0 and start_time is not None and end_time is not None:
        memo_key = _security_memo_key(src, start_time, end_time, filters, bucket_seconds, sections, mask_ips)
        cached = _memo.get_with_telemetry(memo_key)
        if cached is not None:
            return dict(cached)

    def _compute() -> dict:
        if memo_key is not None:
            cached_inner = _memo.get_with_telemetry(memo_key)
            if cached_inner is not None:
                return dict(cached_inner)
        timer = SectionTimer()
        section_timings = timer.entries
        source_name = src["name"]
        table_name = _safe_table(source_name)
        runner = QueryRunner(con, src)
        _t = _time.perf_counter()
        actual_cols = runner.get_schema_cols()
        timer.mark("get_schema_cols", _t, activity="overhead", current_execution=True)
        if not actual_cols:
            return empty_security_aggregates(section_timings=section_timings, **runner.telemetry())
        _t = _time.perf_counter()
        params, where_clause = build_where_clause(start_time, end_time, filters, actual_cols, inline_params=True)
        timer.mark("build_where_clause", _t, activity="overhead", current_execution=True)
        res = _build_security_response(
            runner,
            src,
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
            mask_ips=mask_ips,
        )
        if memo_key is not None:
            _memo.put(memo_key, dict(res))
        return res

    if memo_key is not None:
        return _memo.coalesce(f"security_agg:{memo_key}", _compute)
    return _memo.result(_compute(), state="bypass")


def _build_security_response(
    runner: QueryRunner,
    src: dict,
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
    mask_ips: bool = False,
) -> dict:
    timer = SectionTimer(section_timings)
    section_timings = timer.entries

    def _want(name: str) -> bool:
        return sections is None or name in sections

    def _populate_top_bots(shared_tmp: str | None = None) -> None:
        _t0 = _time.perf_counter()
        try:
            tb = get_top_bots(
                runner.con, src, start_time, end_time, filters or {}, shared_temp_table=shared_tmp, parent_runner=runner
            )
            results["bots"] = tb.get("bots", [])
            results["ngwaf_bots"] = tb.get("ngwaf_bots", [])
        except Exception as e:
            _logger.error("[security] get_top_bots failed in aggregate: %s", e)
        timer.mark("top_bots", _t0, activity="overhead", scope="subtree", current_execution=True)

    results = {**runner.telemetry()}
    rollup_eligible = (
        not filters
        and start_time is not None
        and end_time is not None
        and _window_eligible_for_rollup(start_time, end_time)
    )

    def _can_rollup(col: str) -> bool:
        return rollup_eligible and col in actual_cols and _has_rollup_coverage(src, col, start_time, end_time)

    use_ipv6_rollup = _can_rollup("is_ipv6")
    use_proxy_rollup = _can_rollup("p_type")
    use_top_bots_rollup = _can_rollup("ua")
    if not filters:
        runner.begin_shared_active_hour_temps(
            seed_fields=("tls_ciphers_sha", "p_type", "is_ipv6", "ua", "waf_req_id", "waf_sig")
        )
    needed_cols: set[str] = set()
    live_sections: set[str] = set()

    def _need(section: str, cols: set[str]) -> None:
        live_sections.add(section)
        needed_cols.update(cols)

    _want_ngwaf_bots = _want("ngwaf_verified_bots") or _want("ngwaf_verified_bots_ts")
    if _want_ngwaf_bots:
        try:
            results["ngwaf_configured"] = bool(svcconfig.get_ngwaf_workspace_id(src.get("service_id", "")))
        except Exception:
            results["ngwaf_configured"] = False
    _ngwaf_attached = False
    if _want_ngwaf_bots and "waf_req_id" in actual_cols:
        try:
            _ngwaf_attached = ensure_ngwaf_bots_materialized(runner.con, "ngwaf_cache")
        except Exception:
            _logger.debug("_build_security_response: ensure_ngwaf_bots_materialized failed (ignored)", exc_info=True)

    def _shape_ngwaf_vb_ts_row(r: Any) -> dict:
        return {"time": safe_iso(r[0]), "bot_name": r[1], "count": r[2]}

    if _ngwaf_attached:
        if _want("ngwaf_verified_bots"):
            _t = _time.perf_counter()
            rolled_nb = runner.try_ngwaf_verified_bots_from_rollup(start_time, end_time, has_filters=bool(filters))
            if rolled_nb is not None:
                timer.mark("ngwaf_verified_bots_rollup", _t)
                results["ngwaf_verified_bots"] = rolled_nb
            else:
                _need("ngwaf_verified_bots", {"waf_req_id"})
        if _want("ngwaf_verified_bots_ts"):
            _t = _time.perf_counter()
            rolled_nb_ts = runner.try_ngwaf_verified_bots_ts_from_rollup(
                start_time, end_time, bucket_seconds=bucket_seconds, has_filters=bool(filters)
            )
            if rolled_nb_ts is not None:
                timer.mark("ngwaf_verified_bots_ts_rollup", _t)
                results["ngwaf_verified_bots_ts"] = [_shape_ngwaf_vb_ts_row(r) for r in rolled_nb_ts]
            else:
                _need("ngwaf_verified_bots_ts", {"timestamp", "waf_req_id"})
    elif _want_ngwaf_bots:
        results["ngwaf_verified_bots"] = []
        results["ngwaf_verified_bots_ts"] = []
    if _want("verified_bots_ts"):
        if "waf_sig" in actual_cols:
            _need("verified_bots_ts", {"timestamp", "waf_sig"})
        else:
            results["verified_bots_ts"] = []
    _FP_RESULT_KEYS = (("tls_ciphers_sha", "tls_fingerprints"),)
    _wanted_fp_keys: set[str] = {result_key for _col, result_key in _FP_RESULT_KEYS if _want(result_key)}
    coverage_cols: list[str] = []
    schema_eligible_fp_cols = [
        c for c, rk in _FP_RESULT_KEYS if c in actual_cols and "ip" in actual_cols and rk in _wanted_fp_keys
    ]
    rollup_served_fp_cols: set[str] = set()
    if not filters and schema_eligible_fp_cols == ["tls_ciphers_sha"]:
        try:
            _t = _time.perf_counter()
            cov_pre = runner.try_security_coverage_from_rollup(start_time, end_time, has_filters=False)
            if cov_pre is not None and cov_pre[1] == 0:
                timer.mark(
                    "fingerprint_coverage_rollup", _t, source="rollup", activity="compute", current_execution=True
                )
                results["tls_fingerprints"] = []
                rollup_served_fp_cols.add("tls_ciphers_sha")
                coverage_cols.append("tls_ciphers_sha")
                if _want("fingerprint_coverage"):
                    results["fingerprint_coverage"] = {"tls_ciphers_sha": 0.0}
        except Exception:
            _logger.debug("[security] pre-check coverage rollup failed", exc_info=True)
    if not filters and schema_eligible_fp_cols and len(rollup_served_fp_cols) < len(schema_eligible_fp_cols):
        try:
            _t = _time.perf_counter()
            top_n_rows, _ = runner.execute_top_n_rollups(
                schema_eligible_fp_cols,
                start_time,
                end_time,
                limit=20,
                per_field_limits={c: 20 for c in schema_eligible_fp_cols},
                actual_cols=actual_cols,
            )
            timer.mark(
                "fingerprints_rollup_top_n",
                _t,
                source="rollup",
                activity="compute",
                current_execution=True,
                outcome="read",
            )
            by_field: dict[str, list[tuple[str, int]]] = {}
            for field, value, count in top_n_rows:
                if value == "__other__" or not isinstance(value, str):
                    continue
                by_field.setdefault(field, []).append((value, int(count)))
            if by_field:
                _t = _time.perf_counter()
                ip_spread, _spread_meta = runner.execute_ip_spread_rollups(
                    schema_eligible_fp_cols, start_time, end_time
                )
                timer.mark(
                    "fingerprints_rollup_ip_spread",
                    _t,
                    source="rollup",
                    activity="compute",
                    current_execution=True,
                    outcome="read",
                )
                for col, result_key in _FP_RESULT_KEYS:
                    if col not in by_field:
                        continue
                    candidate_rows: list[dict] = []
                    has_nonzero_ip_count = False
                    for value, count in by_field[col]:
                        ip_count = int(ip_spread.get((col, value), 0))
                        if ip_count > 0:
                            has_nonzero_ip_count = True
                        candidate_rows.append(shape_security_fingerprint_row(value, ip_count, count))
                    if not has_nonzero_ip_count:
                        continue
                    results[result_key] = candidate_rows
                    coverage_cols.append(col)
                    rollup_served_fp_cols.add(col)
        except Exception:
            _logger.debug("[security] fingerprint rollup path failed; falling back to live SQL", exc_info=True)
    for col, result_key in _FP_RESULT_KEYS:
        if result_key not in _wanted_fp_keys or col in rollup_served_fp_cols:
            continue
        if col in actual_cols and "ip" in actual_cols:
            _need("tls_fingerprints", {col, "ip"})
            coverage_cols.append(col)
        else:
            results[result_key] = []
    if coverage_cols and _want("fingerprint_coverage") and "fingerprint_coverage" not in results:
        fp_cols_in_temp = any(c in needed_cols for c in coverage_cols)
        if not fp_cols_in_temp and coverage_cols == ["tls_ciphers_sha"]:
            _t = _time.perf_counter()
            cov = runner.try_security_coverage_from_rollup(start_time, end_time, has_filters=bool(filters))
            if cov is not None:
                timer.mark(
                    "fingerprint_coverage_rollup", _t, source="rollup", activity="compute", current_execution=True
                )
                total_rows, tls_populated = cov
                results["fingerprint_coverage"] = {
                    "tls_ciphers_sha": (tls_populated / total_rows if total_rows > 0 else 0.0)
                }
            else:
                _need("fingerprint_coverage", set(coverage_cols))
        else:
            _need("fingerprint_coverage", set(coverage_cols))

    def _try_rollup_or_need(
        sec_name: str, req_col: str, need_cols: set[str], reader_fn: Callable[[], Any], timer_label: str
    ) -> None:
        if not _want(sec_name):
            return
        if req_col not in actual_cols:
            results[sec_name] = []
            return
        _t = _time.perf_counter()
        rolled = reader_fn()
        timer.mark(
            timer_label,
            _t,
            source="rollup",
            activity="compute" if rolled is not None else "overhead",
            current_execution=True,
            outcome="served" if rolled is not None else "fallback",
        )
        if rolled is not None:
            results[sec_name] = rolled
        else:
            _need(sec_name, need_cols)

    def _try_prechecked_rollup(
        sec_name: str,
        req_col: str,
        use_rollup: bool,
        need_cols: set[str],
        reader_fn: Callable[[], Any],
        timer_label: str,
        warn_msg: str,
    ) -> None:
        if not _want(sec_name):
            return
        if req_col not in actual_cols:
            results[sec_name] = []
            return
        if use_rollup:
            _t = _time.perf_counter()
            rolled = reader_fn()
            timer.mark(
                timer_label,
                _t,
                source="rollup",
                activity="compute" if rolled is not None else "overhead",
                current_execution=True,
                outcome="served" if rolled is not None else "unavailable",
            )
            if rolled is None:
                _logger.warning(warn_msg)
                results[sec_name] = []
            else:
                results[sec_name] = rolled
        else:
            _need(sec_name, need_cols)

    _has_filters = bool(filters)
    _try_rollup_or_need(
        "req_size_dist",
        "req_header_bytes",
        {"req_header_bytes"},
        lambda: runner.try_security_req_size_from_rollup(start_time, end_time, has_filters=_has_filters),
        "req_size_dist_rollup",
    )
    _try_rollup_or_need(
        "top_ips_header",
        "req_header_bytes",
        {"ip", "req_header_bytes"},
        lambda: runner.try_security_top_ips_from_rollup(start_time, end_time, has_filters=_has_filters),
        "top_ips_by_header_rollup",
    )
    _try_rollup_or_need(
        "conn_reuse_dist",
        "conn_requests",
        {"conn_requests"},
        lambda: runner.try_security_conn_reuse_from_rollup(start_time, end_time, has_filters=_has_filters),
        "conn_reuse_dist_rollup",
    )
    _try_prechecked_rollup(
        "ipv6_adoption",
        "is_ipv6",
        use_ipv6_rollup,
        {"timestamp", "is_ipv6"},
        lambda: _ipv6_per_hour_from_rollups(runner, src, start_time, end_time, actual_cols=actual_cols),
        "ipv6_adoption_rollup",
        "[security] ipv6 rollup pre-check passed but per-hour read returned None; rendering empty card",
    )
    _try_prechecked_rollup(
        "proxy_dist",
        "p_type",
        use_proxy_rollup,
        {"p_type"},
        lambda: _proxy_dist_from_rollups(runner, start_time, end_time, actual_cols=actual_cols),
        "proxy_dist_rollup",
        "[security] proxy rollup pre-check passed but reader returned None; rendering empty card",
    )
    _wk_active = False
    _wk_rows: list | None = None
    if _want("wellknown_bots") and "ua" in actual_cols and "ip" in actual_cols:
        _wk_active = True
        if start_time and end_time:
            _t = _time.perf_counter()
            try:
                from backend.core.rollups import read_wellknown_bots_rollup

                if not svcconfig.is_durable_serving_mode(src):
                    _wk_rows = read_wellknown_bots_rollup(src, start_time, end_time)
            except Exception:
                _wk_rows = None
            if _wk_rows is not None:
                timer.mark(
                    "wellknown_bots_rollup_read", _t, source="rollup", activity="compute", current_execution=True
                )
        if _wk_rows is None:
            _need("wellknown_bots", {"ua", "ip"})
    elif _want("wellknown_bots"):
        results["wellknown_bots"] = []
    if _want("bot_efficiency") and "ua" in actual_cols and "ip" in actual_cols:
        _t = _time.perf_counter()
        be_rows = runner.try_security_bot_efficiency_from_rollup(start_time, end_time, has_filters=bool(filters))
        if be_rows is not None:
            results["bot_efficiency"] = summarize_bot_efficiency_rows(be_rows)
            timer.mark("bot_efficiency_rollup", _t, source="rollup", activity="compute", current_execution=True)
        else:
            be_cols = {"ua", "ip"}
            if "referer" in actual_cols:
                be_cols.add("referer")
            if "resp_bytes" in actual_cols:
                be_cols.add("resp_bytes")
            _need("bot_efficiency", be_cols)
    elif _want("bot_efficiency"):
        results["bot_efficiency"] = []
    if _want("top_bots"):
        if use_top_bots_rollup:
            _populate_top_bots()
        else:
            _need("top_bots", {"ua", "waf_req_id"})
    if _want("proxies"):
        proxy_cols = {"ip", "pop", "rtt_min", "tcp_rtt", "lat", "lon", "asn"}
        px_rollup: dict | None = None
        _t = _time.perf_counter()
        if proxy_cols <= set(actual_cols):
            px_rollup = runner.try_security_proxies_from_rollup(start_time, end_time, has_filters=bool(filters))
        if px_rollup is not None:
            results.update(
                _finish_proxies(
                    src["name"],
                    px_rollup["stats"],
                    px_rollup["quality_rows"],
                    px_rollup["isps"],
                    px_rollup["clients"],
                    mask_ips,
                )
            )
            timer.mark("proxies_rollup", _t, source="rollup", activity="compute", current_execution=True)
        else:
            _need("proxies", proxy_cols | {"country", "city"})
    temp_table: str | None = None
    if needed_cols:
        ngwaf_pair = {"ngwaf_verified_bots", "ngwaf_verified_bots_ts"}
        ngwaf_only = bool(live_sections) and live_sections <= ngwaf_pair and "waf_req_id" in actual_cols
        vbts_only = live_sections == {"verified_bots_ts"} and "waf_sig" in actual_cols
        wc = where_clause
        if ngwaf_only and where_clause.strip():
            wc = f"{where_clause} AND waf_req_id IS NOT NULL"
        elif vbts_only and where_clause.strip():
            wc = f"{where_clause} AND waf_sig IS NOT NULL"
        cols = [c for c in _TEMP_COL_ORDER if c in needed_cols]
        _t = _time.perf_counter()
        temp_table = runner.create_filtered_temp_table(cols, actual_cols, table_name, wc, params)
        timer.mark("temp_table_create", _t, source="live", activity="compute", current_execution=True)
        if ngwaf_only:
            timer.mark(
                "security:temp_ngwaf_narrowed", _time.perf_counter(), activity="overhead", current_execution=True
            )
    else:
        timer.mark("security:temp_skipped", _time.perf_counter(), activity="skipped", current_execution=True)

    def _live_fetch(q: str, mark_label: str, shape: Callable[[Any], dict]) -> list[dict]:
        _t0 = _time.perf_counter()
        res = runner.execute(q).fetchall()
        timer.mark(mark_label, _t0, source="live", activity="compute", current_execution=True)
        return [shape(r) for r in res]

    try:
        if "verified_bots_ts" in live_sections and temp_table:
            _t = _time.perf_counter()
            rolled_vbts = runner.try_verified_bots_ts_from_rollup(
                start_time, end_time, temp_table=temp_table, bucket_seconds=bucket_seconds, has_filters=bool(filters)
            )
            if rolled_vbts is not None:
                timer.mark("verified_bots_ts_rollup", _t, source="rollup", activity="compute", current_execution=True)
                vbts_rows = rolled_vbts
            else:
                timer.mark(
                    "verified_bots_ts_rollup",
                    _t,
                    source="rollup",
                    activity="overhead",
                    current_execution=True,
                    outcome="fallback",
                )
                _t = _time.perf_counter()
                q = SQL.VERIFIED_BOTS_TS.format(bucket_seconds=bucket_seconds, temp_table=temp_table)
                vbts_rows = runner.execute(q).fetchall()
                timer.mark("verified_bots_ts", _t, source="live", activity="compute", current_execution=True)
            results["verified_bots_ts"] = [{"time": safe_iso(r[0]), "bot_type": r[1], "count": r[2]} for r in vbts_rows]
        if (
            _ngwaf_attached
            and temp_table
            and ("ngwaf_verified_bots" in live_sections or "ngwaf_verified_bots_ts" in live_sections)
        ):
            try:
                if "ngwaf_verified_bots" in live_sections:
                    results["ngwaf_verified_bots"] = _live_fetch(
                        SQL.NGWAF_VERIFIED_BOTS.format(temp_table=temp_table),
                        "ngwaf_verified_bots",
                        lambda r: shape_security_ngwaf_bot_row(r[0], r[1], r[2], r[3]),
                    )
                if "ngwaf_verified_bots_ts" in live_sections:
                    results["ngwaf_verified_bots_ts"] = _live_fetch(
                        SQL.NGWAF_VERIFIED_BOTS_TS.format(bucket_seconds=bucket_seconds, temp_table=temp_table),
                        "ngwaf_verified_bots_ts",
                        _shape_ngwaf_vb_ts_row,
                    )
            except Exception as e:
                _logger.error("[security] NGWAF bot join failed: %s", e)
                if "ngwaf_verified_bots" in live_sections:
                    results["ngwaf_verified_bots"] = []
                if "ngwaf_verified_bots_ts" in live_sections:
                    results["ngwaf_verified_bots_ts"] = []
        for col, result_key in _FP_RESULT_KEYS:
            if result_key not in _wanted_fp_keys or col in rollup_served_fp_cols:
                continue
            if temp_table and col in actual_cols and "ip" in actual_cols:
                results[result_key] = _live_fetch(
                    SQL.FINGERPRINT_TOP_N.format(col=col, temp_table=temp_table),
                    result_key,
                    lambda r: shape_security_fingerprint_row(r[0], r[1], r[2]),
                )
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
                timer.mark("fingerprint_coverage", _t, source="live", activity="compute", current_execution=True)
                if row:
                    total = float(row[0]) if row[0] else 0.0
                    for i, c in enumerate(coverage_cols):
                        populated = float(row[i + 1]) if row[i + 1] is not None else 0.0
                        fingerprint_coverage[c] = populated / total if total > 0 else 0.0
            except Exception:
                for c in coverage_cols:
                    fingerprint_coverage[c] = 0.0
            results["fingerprint_coverage"] = fingerprint_coverage
        elif _want("fingerprint_coverage") and "fingerprint_coverage" not in results:
            results["fingerprint_coverage"] = {}
        if temp_table:
            for _sec, _sql, _mark, _shape in (
                (
                    "req_size_dist",
                    SQL.REQ_HEADER_SIZE_DIST.format(temp_table=temp_table),
                    "req_size_dist",
                    lambda r: {"bucket": r[0], "count": r[1]},
                ),
                (
                    "top_ips_header",
                    SQL.TOP_IPS_BY_MAX_HEADER.format(temp_table=temp_table),
                    "top_ips_by_header",
                    lambda r: {"ip": r[0], "max_header": r[1]},
                ),
                (
                    "conn_reuse_dist",
                    SQL.CONN_REUSE_DIST.format(temp_table=temp_table),
                    "conn_reuse_dist",
                    lambda r: {"bucket": r[0], "count": r[1]},
                ),
                (
                    "ipv6_adoption",
                    SQL.IPV6_ADOPTION_TS.format(time_bucket_select=time_bucket_select("1 hour"), temp_table=temp_table),
                    "ipv6_adoption",
                    lambda r: {"time": safe_iso(r[0]), "pct": r[1]},
                ),
                (
                    "proxy_dist",
                    SQL.PROXY_TYPE_DIST.format(temp_table=temp_table),
                    "proxy_dist",
                    lambda r: {"type": r[0], "count": r[1]},
                ),
            ):
                if _sec in live_sections:
                    results[_sec] = _live_fetch(_sql, _mark, _shape)
        if _wk_active:
            try:
                if _wk_rows is None and temp_table:
                    from backend.utils.bot_sources import get_bot_regex_pattern

                    _t = _time.perf_counter()
                    pattern = get_bot_regex_pattern(500)
                    if pattern:
                        pattern_sql = escape_sql_literal(pattern)
                        prefilter = f"WHERE ua IS NOT NULL AND ip IS NOT NULL AND regexp_matches(ua, '{pattern_sql}')"
                    else:
                        prefilter = "WHERE ua IS NOT NULL AND ip IS NOT NULL"
                    q = SQL.WELLKNOWN_BOTS_UA_IP.format(temp_table=temp_table, prefilter=prefilter)
                    _wk_rows = runner.execute(q).fetchall()
                    timer.mark("wellknown_bots_query", _t, source="live", activity="compute", current_execution=True)
                if _wk_rows is None:
                    results["wellknown_bots"] = []
                else:
                    _t_sum = _time.perf_counter()
                    results["wellknown_bots"] = summarize_wellknown_bot_rows(_wk_rows)
                    timer.mark("wellknown_bots_summarize", _t_sum, activity="overhead", current_execution=True)
            except Exception as e:
                _logger.error("[security] well-known bots query failed: %s", e)
                results["wellknown_bots"] = []
        if "bot_efficiency" in live_sections and temp_table:
            try:
                _t = _time.perf_counter()
                resp_bytes_expr = coalesce("t.resp_bytes", "0") if "resp_bytes" in actual_cols else "0"
                referer_expr = "t.referer" if "referer" in actual_cols else "CAST(NULL AS VARCHAR)"
                q = SQL.BOT_EFFICIENCY.format(
                    temp_table=temp_table, resp_bytes_expr=resp_bytes_expr, referer_expr=referer_expr
                )
                res = runner.execute(q).fetchall()
                timer.mark("bot_efficiency", _t, source="live", activity="compute", current_execution=True)
                results["bot_efficiency"] = summarize_bot_efficiency_rows(
                    [(str(r[0] or ""), str(r[1] or ""), int(r[2] or 0), int(r[3] or 0)) for r in res]
                )
            except Exception as e:
                _logger.error("[security] bot_efficiency query failed: %s", e)
                results["bot_efficiency"] = []
        if "top_bots" in live_sections and temp_table:
            _populate_top_bots(shared_tmp=temp_table)
        if "proxies" in live_sections and temp_table:
            _t = _time.perf_counter()
            try:
                px = get_security_proxies(
                    runner.con,
                    src,
                    start_time,
                    end_time,
                    filters,
                    shared_temp_table=temp_table,
                    mask_ips=mask_ips,
                    parent_runner=runner,
                )
                for k, v in px.items():
                    if k not in ("section_timings", "debug_calls", "debug_queries"):
                        results[k] = v
            except Exception as e:
                _logger.error("[security] get_security_proxies failed in aggregate: %s", e)
            timer.mark("proxies", _t, activity="overhead", scope="subtree", current_execution=True)
    finally:
        runner.end_shared_active_hour_temps()
        _drop_temp_table(runner, temp_table, "_build_security_response")
    for name, is_list in _SECTION_DEFAULTS:
        if _want(name) and name not in results:
            results[name] = [] if is_list else {}
    if _want("top_bots"):
        results.setdefault("bots", [])
        results.setdefault("ngwaf_bots", [])
    if _want("proxies") and "active_proxies_count" not in results:
        results.update(_empty_proxies_payload())
    if mask_ips and "top_ips_header" in results and results["top_ips_header"]:
        results["top_ips_header"] = [
            shape_security_top_ip_header_row(r.get("ip"), r.get("max_header"), mask_ips=True)
            for r in results["top_ips_header"]
        ]
    results["section_timings"] = section_timings
    return results


for _fn in (_security_memo_key, peek_security_aggregates, get_security_aggregates, _build_security_response):
    _fn.__module__ = "backend.repositories.security"
