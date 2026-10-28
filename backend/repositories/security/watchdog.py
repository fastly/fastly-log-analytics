from __future__ import annotations

import hashlib as _hashlib
import json as _json
import logging
import os
import re
import time as _time
from contextlib import nullcontext
from datetime import UTC, datetime
from typing import Any

import duckdb

from backend.core.duckdb import escape_sql_literal
from backend.models.analytics import ReaderCoverage
from backend.models.common import FiltersDict
from backend.repositories import dashboard as _dashboard_repo
from backend.repositories._base import (
    QueryRunner,
    SectionTimer,
    _safe_table,
    empty_schema_response,
    ensure_ngwaf_bots_materialized,
    safe_iso,
)
from backend.repositories.security import sql as SQL
from backend.repositories.utils.filters import build_where_clause
from backend.repositories.utils.response_cache import (
    cache_result,
    serialize_filters_for_key,
)
from backend.repositories.utils.response_shaping import (
    empty_security_proxies,
    finish_security_proxies,
    parse_asn_int,
    shape_security_ngwaf_top_bot_row,
)
from backend.utils.date_utils import parse_window

_logger = logging.getLogger(__name__)

_ROLLUP_MIN_WINDOW_SECONDS = 86400
_BOT_UA_CLASSIFY_LIMIT = int(os.getenv("BOT_UA_CLASSIFY_LIMIT", "2000"))
_RDNS_LOOKUP_LIMIT = int(os.getenv("SECURITY_RDNS_LOOKUP_LIMIT", "1000"))
_RDNS_ENQUEUE_LIMIT = int(os.getenv("SECURITY_RDNS_ENQUEUE_LIMIT", "500"))

_empty_proxies_payload = empty_security_proxies
_parse_asn_int = parse_asn_int
_finish_proxies = finish_security_proxies


def _window_eligible_for_rollup(start_time: str | None, end_time: str | None) -> bool:
    if not start_time or not end_time:
        return False
    try:
        window = parse_window(start_time, end_time)
    except Exception:
        return False
    if window is None:
        return False
    st_dt, et_dt = window
    return (et_dt - st_dt).total_seconds() >= _ROLLUP_MIN_WINDOW_SECONDS


def _has_rollup_coverage(src: dict, field: str, start_time: str | None, end_time: str | None) -> bool:
    from backend.core.rollups.recompute import enumerate_closed_hour_field_rollups

    w = enumerate_closed_hour_field_rollups(src, field, start_time, end_time, stop_on_first=True)
    return bool(w and (w.bundled_paths or w.per_field_paths))


def _ipv6_per_hour_from_rollups(
    runner: QueryRunner,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    *,
    actual_cols: list[str] | None = None,
) -> list[dict] | None:
    from backend.core.rollups._common import _safe_table_for
    from backend.core.rollups.recompute import enumerate_closed_hour_field_rollups

    w = enumerate_closed_hour_field_rollups(src, "is_ipv6", start_time, end_time)
    if w is None or (not w.bundled_paths and not w.per_field_paths):
        return None
    bundled_paths = w.bundled_paths
    per_field_paths = w.per_field_paths
    st_dt, et_dt = w.st_dt, w.et_dt
    active_dt, active_end_dt = w.active_dt, w.active_end_dt
    branches = [
        "SELECT hour, value, count FROM read_parquet(["
        + ", ".join("'" + escape_sql_literal(p) + "'" for p in paths)
        + "], hive_partitioning=1) WHERE field = 'is_ipv6'"
        for paths in (bundled_paths, per_field_paths)
        if paths
    ]
    q = "SELECT hour, value, SUM(count) AS c FROM (" + " UNION ALL ".join(branches) + ") GROUP BY hour, value"
    try:
        rows = runner.execute(q).fetchall()
    except Exception:
        return None
    per_hour: dict[str, dict[str, int]] = {}
    for hour, value, count in rows:
        b = per_hour.setdefault(hour, {"true": 0, "total": 0})
        b["total"] += int(count)
        if value == "true":
            b["true"] += int(count)
    if not per_hour:
        return None
    if et_dt > active_dt:
        live_start = max(active_dt, st_dt)
        live_end = min(active_end_dt, et_dt)
        if live_end > live_start:
            base_table = _safe_table_for(src)
            if base_table:
                tmp = None
                try:
                    if hasattr(runner, "_create_active_hour_temp_direct"):
                        cols_for_tmp = actual_cols if actual_cols is not None else ["timestamp", "is_ipv6"]
                        tmp = runner._create_active_hour_temp_direct(["is_ipv6"], cols_for_tmp, live_start, live_end)
                    if tmp == "__empty__":
                        rows_live = []
                    else:
                        live_from = (
                            f'"{tmp}"'
                            if tmp is not None
                            else (
                                f'"{base_table}" '
                                f"WHERE timestamp >= '{live_start.isoformat()}' "
                                f"AND timestamp < '{live_end.isoformat()}'"
                            )
                        )
                        q_live = (
                            f"SELECT strftime(timestamp, '%Y-%m-%d-%H') AS hour, "
                            f"SUM(CASE WHEN is_ipv6 THEN 1 ELSE 0 END) AS true_c, "
                            f"count(*) AS total_c FROM {live_from} GROUP BY 1"
                        )
                        rows_live = runner.execute(q_live).fetchall()
                    for hour, true_c, total_c in rows_live:
                        b = per_hour.setdefault(hour, {"true": 0, "total": 0})
                        b["true"] += int(true_c) if true_c is not None else 0
                        b["total"] += int(total_c) if total_c is not None else 0
                except Exception:
                    _logger.debug("[security] ipv6 active-hour merge failed", exc_info=True)
                finally:
                    if tmp and tmp != "__empty__" and hasattr(runner, "release_active_direct_temp"):
                        runner.release_active_direct_temp(tmp)
    result: list[dict] = []
    for hour in sorted(per_hour):
        b = per_hour[hour]
        pct = (b["true"] / b["total"] * 100.0) if b["total"] else 0.0
        dt = datetime(int(hour[:4]), int(hour[5:7]), int(hour[8:10]), int(hour[11:13]), 0, 0, tzinfo=UTC)
        result.append({"time": safe_iso(dt), "pct": pct})
    return result


def _proxy_dist_from_rollups(
    runner: QueryRunner, start_time: str | None, end_time: str | None, *, actual_cols: list[str] | None = None
) -> list[dict] | None:
    try:
        extra_kwargs: dict[str, Any] = {"actual_cols": actual_cols} if actual_cols is not None else {}
        rolled, _ = runner.execute_top_n_rollups(["p_type"], start_time, end_time, limit=500, **extra_kwargs)
    except Exception:
        return None
    if not rolled:
        return None
    by_val: dict[str, int] = {}
    for _field, value, count in rolled:
        if not isinstance(value, str) or value == "" or value == "__other__":
            continue
        by_val[value] = by_val.get(value, 0) + int(count)
    if not by_val:
        return None
    return [{"type": v, "count": c} for v, c in sorted(by_val.items(), key=lambda kv: kv[1], reverse=True)]


def classify_ua_counts(rows: list[tuple[str, int]], n: int) -> list[dict]:
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


def summarize_wellknown_bot_rows(rows: list) -> list[dict]:
    if not rows:
        return []
    from backend.utils.bot_sources import build_matcher, classify, enqueue, get_hostnames

    match_ua = build_matcher()
    bot_agg: dict[str, dict] = {}
    new_ips: set[str] = set()
    ua_matches: dict[str, list[dict]] = {}
    matched_rows: list[tuple[list[dict], str, int]] = []
    ip_totals: dict[str, int] = {}
    for ua_val, ip_val, cnt in rows:
        if not ua_val or not ip_val:
            continue
        matches = ua_matches.get(ua_val)
        if matches is None:
            matches = list(match_ua(ua_val))
            ua_matches[ua_val] = matches
        if not matches:
            continue
        cnt_int = int(cnt or 0)
        matched_rows.append((matches, ip_val, cnt_int))
        ip_totals[ip_val] = ip_totals.get(ip_val, 0) + cnt_int
    if not matched_rows:
        return []
    if len(ip_totals) > _RDNS_LOOKUP_LIMIT:
        top_ips = [ip for ip, _ in sorted(ip_totals.items(), key=lambda kv: kv[1], reverse=True)[:_RDNS_LOOKUP_LIMIT]]
    else:
        top_ips = list(ip_totals)
    hostnames = get_hostnames(top_ips)
    top_ips_set = set(top_ips)
    classify_cache: dict[tuple[str, str], str] = {}
    for matches, ip_val, cnt in matched_rows:
        hostname, status, fcrdns_verified = hostnames.get(ip_val, (None, "pending", False))
        if ip_val not in top_ips_set and ip_val not in hostnames and len(new_ips) < _RDNS_ENQUEUE_LIMIT:
            new_ips.add(ip_val)
        for entry in matches:
            bot_id = entry.get("id", "unknown")
            cache_key = (bot_id, ip_val)
            state = classify_cache.get(cache_key)
            if state is None:
                verification = entry.get("verification", {})
                verification_domains = verification.get("domains", [])
                verification_cidrs = tuple(verification.get("cidrs") or ())
                state = classify(ip_val, hostname, status, fcrdns_verified, verification_domains, verification_cidrs)
                classify_cache[cache_key] = state
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
    if new_ips:
        enqueue(list(new_ips))
    sorted_bots = sorted(bot_agg.values(), key=lambda x: x["request_count"], reverse=True)[:50]
    for b in sorted_bots:
        total = b["request_count"]
        covered = b["verified_count"] + b["impersonator_count"]
        b["verification_coverage"] = round(covered / total, 3) if total else 0.0
    return sorted_bots


_VOWEL_TABLE = str.maketrans("", "", "aeiou")


def _stems_match(bot_stem: str, ref_stem: str) -> bool:
    if not bot_stem or not ref_stem:
        return False
    if bot_stem == ref_stem:
        return True
    b_cons = bot_stem.translate(_VOWEL_TABLE)
    r_cons = ref_stem.translate(_VOWEL_TABLE)
    if len(b_cons) >= 3 and b_cons == r_cons:
        return True
    return len(bot_stem) >= 3 and len(ref_stem) >= 3 and (bot_stem in ref_stem or ref_stem in bot_stem)


_GENERIC_BOT_RE = re.compile(
    r"([a-z0-9_.-]*(?:bot|crawler|spider|slurp|archiver|scraper|externalagent|extended)[a-z0-9_.-]*)", re.IGNORECASE
)


def summarize_bot_efficiency_rows(rows: list[tuple[str, str, int, int]]) -> list[dict[str, Any]]:
    from backend.utils.bot_sources import (
        _BOT_TOKEN_SUFFIX_RE,
        build_well_known_bots_matcher,
        extract_entry_referral_stems,
        load_well_known_bots_catalog,
        resolve_bot_display_name,
    )

    matcher = build_well_known_bots_matcher()
    catalog = load_well_known_bots_catalog() or []
    vendor_stems: dict[str, set[str]] = {}
    for entry in catalog:
        if not isinstance(entry, dict):
            continue
        vid = (entry.get("id") or "").split("-")[0].lower()
        if vid:
            vendor_stems.setdefault(vid, set()).update(extract_entry_referral_stems(entry))
    total_site_bytes = 0
    ref_counts: dict[str, int] = {}
    ua_rows: list[tuple[str, int, int]] = []
    for kind, key, cnt, r_bytes in rows:
        cnt_i = int(cnt or 0)
        bytes_i = int(r_bytes or 0)
        if kind == "site":
            total_site_bytes += bytes_i
        elif kind == "ref" and key:
            ref_counts[key.lower()] = ref_counts.get(key.lower(), 0) + cnt_i
        elif kind == "ua" and key:
            ua_rows.append((key, cnt_i, bytes_i))
    bot_agg: dict[str, dict[str, Any]] = {}
    for ua, cnt_i, bytes_i in ua_rows:
        matches = matcher(ua)
        matched_entry = matches[0] if matches else None
        if isinstance(matched_entry, dict):
            bot_id = matched_entry.get("id", "unknown")
            bot_name = bot_id.replace("-", " ").title()
            cats = matched_entry.get("categories") or []
            category = cats[0].replace("-", " ").title() if cats else "Uncategorized"
            vid = bot_id.split("-")[0].lower()
            stems = set(extract_entry_referral_stems(matched_entry)) | vendor_stems.get(vid, set())
        else:
            m = _GENERIC_BOT_RE.search(ua.lower())
            if not m:
                continue
            raw_tok = m.group(1)
            bot_id = raw_tok
            bot_name = resolve_bot_display_name(raw_tok, default="Other Bot")
            category = "General Bot"
            stem = _BOT_TOKEN_SUFFIX_RE.sub("", raw_tok).strip("-_.")
            stems = {stem} if len(stem) >= 3 else set()
        bucket = bot_agg.get(bot_id)
        if bucket is None:
            bucket = {"bot_name": bot_name, "category": category, "request_count": 0, "resp_bytes": 0, "stems": set()}
            bot_agg[bot_id] = bucket
        bucket["request_count"] += cnt_i
        bucket["resp_bytes"] += bytes_i
        bucket["stems"].update(stems)
    denom_bytes = max(total_site_bytes, 1)
    results: list[dict[str, Any]] = []
    for b in bot_agg.values():
        b_stems: set[str] = b["stems"]
        referrals = sum(rcnt for rstem, rcnt in ref_counts.items() if any(_stems_match(bs, rstem) for bs in b_stems))
        req_cnt = int(b["request_count"])
        resp_b = int(b["resp_bytes"])
        ref_denom = max(referrals, 1)
        results.append(
            {
                "bot_name": b["bot_name"],
                "category": b["category"],
                "request_count": req_cnt,
                "resp_bytes": resp_b,
                "bandwidth_pct": round(resp_b * 100.0 / denom_bytes, 2),
                "referrals": referrals,
                "requests_per_referral": round(req_cnt * 1.0 / ref_denom, 1),
                "bytes_per_referral": round(resp_b * 1.0 / ref_denom, 1),
            }
        )
    results.sort(key=lambda x: (x["resp_bytes"], x["request_count"]), reverse=True)
    return results[:25]


def _top_bots_cache_key(
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    n: int,
    shared_temp_table: str | None = None,
) -> str | None:
    if (
        _dashboard_repo.DASHBOARD_CACHE_TTL > 0
        and start_time is not None
        and end_time is not None
        and shared_temp_table is None
    ):
        _key_payload = _json.dumps(
            {
                # Bounds are already resolved; seconds/fractions define distinct scan windows.
                "s": start_time,
                "e": end_time,
                "f": serialize_filters_for_key(filters),
                "n": n,
            },
            separators=(",", ":"),
        )
        return f"top_bots:{_hashlib.sha256(_key_payload.encode()).hexdigest()}:{src['name']}"
    return None


def peek_top_bots(
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    n: int = 10,
) -> dict | None:
    key = _top_bots_cache_key(src, start_time, end_time, filters, n)
    return _dashboard_repo._lookup_dashboard_cache(key) if key is not None else None


def get_top_bots(
    con: duckdb.DuckDBPyConnection,
    src: dict,
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    n: int = 10,
    shared_temp_table: str | None = None,
    parent_runner: QueryRunner | None = None,
) -> dict:
    timer = SectionTimer()
    section_timings = timer.entries
    source_name = src["name"]
    cache_key = _top_bots_cache_key(src, start_time, end_time, filters, n, shared_temp_table)
    if cache_key is not None:
        cached = _dashboard_repo._lookup_dashboard_cache(cache_key)
        if cached is not None:
            return cached
    table_name = _safe_table(source_name)
    runner = parent_runner if parent_runner is not None else QueryRunner(con, src)

    def _wrap_cache(resp: dict) -> dict:
        return cache_result(
            resp,
            state="miss" if cache_key is not None else "bypass",
            ttl_seconds=_dashboard_repo._dashboard_cache.ttl if cache_key is not None else None,
            prewarmer=False,
        )

    _t = _time.perf_counter()
    actual_cols = runner.get_schema_cols()
    timer.mark("top_bots:get_schema_cols", _t)
    if not actual_cols:
        return _wrap_cache(empty_schema_response(bots=[], ngwaf_bots=[], **runner.telemetry()))
    _t = _time.perf_counter()
    params, where_clause = build_where_clause(start_time, end_time, filters, actual_cols, inline_params=True)
    timer.mark("top_bots:build_where_clause", _t)
    arcjet_bots: list[dict] = []
    ngwaf_bots: list[dict] = []

    def _bots_resp() -> dict:
        return {"bots": arcjet_bots, "ngwaf_bots": ngwaf_bots, "section_timings": section_timings, **runner.telemetry()}

    use_rollups = not filters
    if use_rollups:
        runner.begin_shared_active_hour_temps()
    else:
        skipped = []
        if "ua" in actual_cols:
            skipped.append(ReaderCoverage(reader="dashboard.ua_bots", maximum_window_days=None))
        if "waf_req_id" in actual_cols:
            skipped.append(ReaderCoverage(reader="dashboard.ngwaf_bots", maximum_window_days=None))
        window = parse_window(start_time, end_time)
        for observation in skipped:
            observation.eligible, observation.eligibility_reason, observation.outcome = False, "filtered", "ineligible"
            if window is not None:
                observation.window_start, observation.window_end = window[0].isoformat(), window[1].isoformat()
        runner.reader_coverage.extend(skipped)
    try:
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
                    actual_cols=actual_cols,
                    _coverage_reader="dashboard.ua_bots",
                )
                timer.mark("top_bots:ua_rollup_query", _t)
                ua_rollup_rows = [(v, int(c)) for _f, v, c in rolled if v and v != "__other__"]
                if not ua_rollup_rows:
                    ua_rollup_rows = None
            except Exception as e:
                _logger.warning("[security] UA rollup read failed, falling back: %s", e)
                ua_rollup_rows = None
        if ua_rollup_rows is not None:
            _t = _time.perf_counter()
            try:
                arcjet_bots = classify_ua_counts(ua_rollup_rows, n)
            except Exception as e:
                _logger.error("[security] arcjet rollup match failed: %s", e)
            timer.mark("top_bots:arcjet_match", _t)
        ngwaf_attached = False
        if "waf_req_id" in actual_cols:
            try:
                ngwaf_attached = ensure_ngwaf_bots_materialized(con, "ngwaf_top")
            except Exception:
                _logger.debug("get_top_bots: ensure_ngwaf_bots_materialized failed (ignored)", exc_info=True)
        needs_filtered_ua_scan = ua_rollup_rows is None and "ua" in actual_cols
        cols_needed: list[str] = []
        if needs_filtered_ua_scan:
            cols_needed.append("ua")
        if ngwaf_attached and "waf_req_id" in actual_cols:
            cols_needed.append("waf_req_id")

        def _run_ngwaf_join(q: str, q_params: list | None, mark_label: str) -> list[dict]:
            _t0 = _time.perf_counter()
            join_outcome = "error"
            try:
                res = (runner.execute(q, q_params) if q_params is not None else runner.execute(q)).fetchall()
                out = [shape_security_ngwaf_top_bot_row(r[0], r[1], r[2]) for r in res]
                join_outcome = "success"
                return out
            except Exception as e:
                _logger.error("[security] NGWAF top bots failed: %s", e)
                return []
            finally:
                timer.mark(mark_label, _t0)
                timer.entries[-1].update(
                    source="live", activity="compute", current_execution=True, outcome=join_outcome
                )

        if cols_needed == ["waf_req_id"]:
            rolled_ngwaf: list[dict] | None = None
            if use_rollups:
                _t = _time.perf_counter()
                coverage_start = len(runner.reader_coverage)
                rollup_error = False
                try:
                    rolled_ngwaf = runner.try_ngwaf_top_bots_from_rollup(
                        start_time, end_time, has_filters=bool(filters), n=n
                    )
                except Exception as e:
                    _logger.warning("[security] ngwaf_bots rollup read failed, falling back: %s", e)
                    rolled_ngwaf = None
                    rollup_error = True
                timer.mark("top_bots:ngwaf_rollup", _t)
                observed = next(
                    (row for row in runner.reader_coverage[coverage_start:] if row.reader == "dashboard.ngwaf_bots"),
                    None,
                )
                source = "unknown"
                if observed is not None:
                    if observed.outcome in {"served", "read_failed"}:
                        source = "mixed" if observed.live_top_up in {"used", "failed"} else "rollup"
                    rollup_error = rollup_error or observed.outcome == "read_failed" or observed.live_top_up == "failed"
                timer.entries[-1].update(
                    source=source,
                    activity="overhead" if source == "unknown" else "compute",
                    current_execution=True,
                    outcome="error" if rollup_error else "success",
                )
            if rolled_ngwaf is not None:
                ngwaf_bots = rolled_ngwaf
            else:
                ngwaf_bots = _run_ngwaf_join(
                    SQL.NGWAF_TOP_BOTS_JOIN_DIRECT.format(table_name=table_name, where_clause=where_clause, n=n),
                    params,
                    "top_bots:ngwaf_join_direct",
                )
        elif cols_needed:
            _t = _time.perf_counter()
            ctx = (
                nullcontext(shared_temp_table)
                if shared_temp_table
                else runner.temp_table(cols_needed, actual_cols, table_name, where_clause, params)
            )
            with ctx as temp_table:
                timer.mark("top_bots:temp_table_create", _t)
                if temp_table is None:
                    return _bots_resp()
                if needs_filtered_ua_scan:
                    try:
                        _t = _time.perf_counter()
                        q = SQL.TOP_UAS_BY_COUNT.format(temp_table=temp_table, limit=_BOT_UA_CLASSIFY_LIMIT)
                        rows = runner.execute(q).fetchall()
                        timer.mark("top_bots:top_uas_query", _t)
                        _t = _time.perf_counter()
                        arcjet_bots = classify_ua_counts(rows, n)
                        timer.mark("top_bots:arcjet_match", _t)
                    except Exception as e:
                        _logger.error("[security] arcjet top bots failed: %s", e)
                if ngwaf_attached:
                    ngwaf_bots = _run_ngwaf_join(
                        SQL.NGWAF_TOP_BOTS_JOIN.format(temp_table=temp_table, n=n), None, "top_bots:ngwaf_join"
                    )
    finally:
        if use_rollups:
            runner.end_shared_active_hour_temps()
    res_dict = _bots_resp()
    if cache_key is not None and (arcjet_bots or ngwaf_bots):
        _dashboard_repo._dashboard_cache[cache_key] = (_time.time(), res_dict.copy())
    return _wrap_cache(res_dict)


def get_security_proxies(
    con: duckdb.DuckDBPyConnection,
    src: dict[str, Any],
    start_time: Any,
    end_time: Any,
    filters: dict[str, Any] | None = None,
    shared_temp_table: str | None = None,
    mask_ips: bool = False,
    parent_runner: QueryRunner | None = None,
) -> dict[str, Any]:
    source_name = src["name"]
    table_name = _safe_table(source_name)
    runner = parent_runner if parent_runner is not None else QueryRunner(con, src)
    actual_cols = runner.get_schema_cols()
    required = ["ip", "pop", "rtt_min", "tcp_rtt", "lat", "lon", "asn"]
    if not actual_cols or not all(col in actual_cols for col in required):
        return _empty_proxies_payload()
    params, where_clause = build_where_clause(start_time, end_time, filters or {}, actual_cols, inline_params=True)
    optional = ["country", "city"]
    cols_needed = [c for c in required + optional if c in actual_cols]
    ctx = (
        nullcontext(shared_temp_table)
        if shared_temp_table
        else runner.temp_table(cols_needed, actual_cols, table_name, where_clause, params)
    )
    with ctx as temp_table:
        if temp_table is None:
            return _empty_proxies_payload()
        select_country_city_inner = "".join(
            f",\n            {col}" if col in cols_needed else f",\n            CAST(NULL AS VARCHAR) AS {col}"
            for col in optional
        )
        select_country_city_outer = ",\n                country,\n                city"
        combined_rows = runner.execute(
            SQL.GET_SECURITY_PROXIES_COMBINED.format(
                temp_table=temp_table,
                select_country_city_inner=select_country_city_inner,
                select_country_city_outer=select_country_city_outer,
            )
        ).fetchall()
        stats: dict[str, Any] = {}
        quality_rows: list[tuple[Any, Any]] = []
        raw_isps: list[dict[str, Any]] = []
        raw_clients: list[dict[str, Any]] = []
        for r in combined_rows:
            kind = r[0]
            if kind == "stats":
                stats = {
                    "active_proxies_count": r[2] or 0,
                    "total_requests_count": r[3] or 0,
                    "distance_mismatches_count": r[4] or 0,
                }
            elif kind == "quality":
                quality_rows.append((r[1], r[2]))
            elif kind == "isp":
                raw_isps.append({"asn": r[2], "count": r[3]})
            elif kind == "client":
                raw_clients.append(
                    {
                        "ip": r[1],
                        "asn": r[2],
                        "rtt_min_ms": r[5],
                        "tcp_rtt_ms": r[6],
                        "distance_km": r[7],
                        "pop": r[8],
                        "client_lat": r[9],
                        "client_lon": r[10],
                        "pop_lat": r[11],
                        "pop_lon": r[12],
                        "impossible_distance": r[13],
                        "risk_level": r[14],
                        "country": r[15],
                        "city": r[16],
                    }
                )
        return _finish_proxies(source_name, stats, quality_rows, raw_isps, raw_clients, mask_ips)


def get_threat_intel_rows(con: Any, src: dict, table_name: str, start: str, end: str) -> list[tuple]:
    from backend.utils.telemetry import track_query

    cols = set(QueryRunner(con, src).get_schema_cols())
    with track_query(con, SQL.threat_intel_sql(table_name, cols), [start, end], "threat_intel") as cursor:
        return cursor.fetchall()


for _fn in (
    _window_eligible_for_rollup,
    _has_rollup_coverage,
    _ipv6_per_hour_from_rollups,
    _proxy_dist_from_rollups,
    classify_ua_counts,
    summarize_wellknown_bot_rows,
    _stems_match,
    summarize_bot_efficiency_rows,
    _top_bots_cache_key,
    peek_top_bots,
    get_top_bots,
    get_security_proxies,
    get_threat_intel_rows,
):
    _fn.__module__ = "backend.repositories.security"
