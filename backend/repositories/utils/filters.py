"""Parameterized SQL WHERE-clause and geo/pagination helpers for repositories."""

from __future__ import annotations

import contextlib
import logging
import os
import re
from dataclasses import dataclass
from dataclasses import field as _dc_field
from typing import TYPE_CHECKING, Any

from backend.utils.date_utils import parse_iso_utc
from backend.utils.sql_validator import escape_sql_literal

if TYPE_CHECKING:
    from backend.models.common import FiltersDict

_logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class SqlFragment:
    sql: str
    params: dict[str, Any] = _dc_field(default_factory=dict)

    def __str__(self) -> str:
        return self.sql


@dataclass(frozen=True, slots=True)
class FilterClause:
    sql: str
    params: list[Any] = _dc_field(default_factory=list)

    def as_tuple(self) -> tuple[str, list[Any]]:
        return self.sql, list(self.params)


class FilterBuilder:
    def __init__(self, clauses: list[str] | None = None, params: list[Any] | None = None) -> None:
        self.clauses: list[str] = list(clauses or [])
        self.params: list[Any] = list(params or [])

    def add(self, clause: str, *values: Any) -> FilterBuilder:
        if clause:
            self.clauses.append(clause)
            self.params.extend(values)
        return self

    def build(self, default: str = "1=1") -> tuple[str, list[Any]]:
        return (" AND ".join(self.clauses) if self.clauses else default), list(self.params)


_SAFE_COL_RE = re.compile(r"[^\w]")


def _lookup_ngwaf_waf_req_ids(bot_names: list[str], db_path: str | None = None) -> list[str]:
    if not bot_names:
        return []
    q = f"SELECT waf_req_id FROM ngwaf_bots WHERE bot_name IN ({', '.join('?' for _ in bot_names)})"
    if db_path and os.path.exists(db_path):
        import sqlite3

        try:
            with contextlib.closing(sqlite3.connect(db_path, timeout=5)) as con:
                return [r[0] for r in con.execute(q, bot_names).fetchall()]
        except Exception as e:
            _logger.warning("[build_where_clause] ngwaf_bots test cache lookup failed: %s", e)
            return []
    from backend.core.metadata import pg_connection

    try:
        with contextlib.closing(pg_connection.get_pg_readonly_connection()) as pconn:
            rows = pconn.execute(q, bot_names).fetchall()
    except Exception as e:
        _logger.warning("[build_where_clause] ngwaf_bots cache lookup failed: %s", e)
        return []
    return [r["waf_req_id"] if isinstance(r, dict) else r[0] for r in rows]


def filter_spec_attr(spec: Any, attr: str) -> Any:
    return spec.get(attr) if isinstance(spec, dict) else getattr(spec, attr, None)


def normalize_filter_key(filter_key: str) -> str:
    col = filter_key
    for prefix in ("xfilter_", "filter_"):
        if col.startswith(prefix):
            col = col[len(prefix) :]
            break
    return _SAFE_COL_RE.sub("", re.sub(r"_\d+$", "", col)).lower()


def _get_utc_date_str(iso_str: str) -> str:
    d = parse_iso_utc(iso_str)
    return d.strftime("%Y-%m-%d") if d else str(iso_str)[:10]


def _get_utc_hour_str(iso_str: str) -> str:
    d = parse_iso_utc(iso_str)
    return d.strftime("%Y-%m-%d-%H") if d else str(iso_str)[:13].replace(" ", "-").replace("T", "-")


def build_where_clause(
    start_time: str | None,
    end_time: str | None,
    filters: FiltersDict,
    actual_cols: list[str] | None = None,
    inline_params: bool = False,
    partition_pruning: bool = False,
    exclude_invalid_ips: bool = True,
) -> tuple[list[Any], str]:
    conditions: list[str] = []
    params: list[Any] = []
    if exclude_invalid_ips and actual_cols is not None and "ip" in actual_cols:
        conditions.append("ip IS NOT NULL AND ip != ''")
    if exclude_invalid_ips and actual_cols is not None and "url" in actual_cols:
        conditions.append("url != '/rum-beacon' AND url NOT LIKE '/rum-beacon' || chr(63) || '%'")

    def _add_param(val: Any) -> str:
        if inline_params:
            if isinstance(val, str):
                cleaned = "".join(ch for ch in val if ch == "\t" or ch == "\n" or ord(ch) >= 0x20)
                return f"'{cleaned.replace('`', '').replace(chr(39), chr(39) + chr(39))}'"
            return str(val)
        params.append(val)
        return "?"

    for bound, op in ((start_time, ">="), (end_time, "<=")):
        if bound:
            conditions.append(f"timestamp {op} CAST({_add_param(bound)} AS TIMESTAMPTZ)")
            if partition_pruning:
                d_str, h_str = _get_utc_date_str(bound), _get_utc_hour_str(bound)
                if len(d_str) == 10 and (actual_cols is None or "dt" in actual_cols):
                    conditions.append(f"dt {op} {_add_param(d_str)}")
                if len(h_str) == 13 and (actual_cols is None or "timestamp_hour" in actual_cols):
                    conditions.append(f"timestamp_hour {op} {_add_param(h_str)}")
    for filter_key, spec in filters.items():
        col = normalize_filter_key(filter_key)
        clean_col = _SAFE_COL_RE.sub("", col)  # strip anything non-word
        is_signals_individual = col == "waf_sig_ind"
        is_bot_name, is_ngwaf_bot_name, is_tunnel_requests = (
            col == "_bot_name",
            col == "_ngwaf_bot_name",
            col == "_tunnel_requests",
        )
        sql_col = "waf_sig" if is_signals_individual else clean_col
        mode = filter_spec_attr(spec, "mode") or "include"
        values = filter_spec_attr(spec, "values") or []
        if (
            actual_cols is not None
            and sql_col not in actual_cols
            and not (is_bot_name or is_ngwaf_bot_name or is_tunnel_requests or is_signals_individual)
        ):
            if sql_col in ("browser", "os", "device") or mode != "exclude":
                conditions.append("FALSE")
            continue
        values = [v for v in values if not (isinstance(v, str) and v.strip() == "")]
        if not values:
            continue
        non_none = [v for v in values if v is not None]
        none_vals = [v for v in values if v is None]
        parts: list[str] = []
        not_prefix = "NOT " if mode == "exclude" else ""
        if is_bot_name:
            try:
                from backend.utils.bot_sources import get_bot_by_id
            except ImportError:
                continue
            for bot_id in non_none:
                bot = get_bot_by_id(str(bot_id))
                if bot is None:
                    continue
                if bot.get("type") == "ip" or "cidrs" in bot.get("verification", {}):
                    if actual_cols is not None and "ip" not in actual_cols:
                        continue
                    cidrs = bot.get("verification", {}).get("cidrs", [])
                    if not cidrs:
                        continue
                    cidr_conditions = []
                    import ipaddress

                    for c in cidrs:
                        try:
                            net_obj = ipaddress.ip_network(c, strict=False)
                            if isinstance(net_obj, ipaddress.IPv4Network):
                                lower_int, upper_int = int(net_obj.network_address), int(net_obj.broadcast_address)
                                ip_int_sql = (
                                    "(CAST(string_split(ip, '.')[1] AS BIGINT) * 16777216 + "
                                    "CAST(string_split(ip, '.')[2] AS BIGINT) * 65536 + "
                                    "CAST(string_split(ip, '.')[3] AS BIGINT) * 256 + "
                                    "CAST(string_split(ip, '.')[4] AS BIGINT))"
                                )
                                cidr_conditions.append(
                                    f"(NOT (ip LIKE '%:%') AND len(string_split(ip, '.')) = 4 AND {ip_int_sql} >= {lower_int} AND {ip_int_sql} <= {upper_int})"
                                )
                        except ValueError:
                            continue
                    if cidr_conditions:
                        parts.append(f"{not_prefix}({' OR '.join(cidr_conditions)})")
                else:
                    if actual_cols is not None and "ua" not in actual_cols:
                        _logger.warning("[build_where_clause] _bot_name filter skipped: 'ua' not in schema")
                        continue
                    raw_patterns = bot.get("pattern", {}).get("accepted", [])
                    if not raw_patterns:
                        continue
                    safe_regex = escape_sql_literal("|".join(raw_patterns))
                    parts.append(f"{not_prefix}regexp_matches(ua, '(?i){safe_regex}')")
        elif is_ngwaf_bot_name:
            if actual_cols is not None and "waf_req_id" not in actual_cols:
                _logger.warning("[build_where_clause] _ngwaf_bot_name filter skipped: 'waf_req_id' not in schema")
                continue
            bot_names = [str(v) for v in non_none if v]
            if bot_names:
                from backend import config as svcconfig

                waf_req_ids = _lookup_ngwaf_waf_req_ids(bot_names, db_path=svcconfig.ngwaf_sqlite_cache_override())
                if waf_req_ids:
                    parts.append(f"waf_req_id {not_prefix}IN ({', '.join(_add_param(v) for v in waf_req_ids)})")
                elif mode != "exclude":
                    parts.append("FALSE")
        elif is_tunnel_requests:
            if actual_cols is not None and not all(
                c in actual_cols for c in ["pop", "lat", "lon", "rtt_min", "tcp_rtt"]
            ):
                _logger.warning("[build_where_clause] _tunnel_requests filter skipped: required columns not in schema")
                parts.append("TRUE" if mode == "exclude" else "FALSE")
            else:
                from backend.utils.pop_utils import build_pop_haversine_case_sql

                pop_distance_case = build_pop_haversine_case_sql()
                cond = f"""
                (
                    ({pop_distance_case} > ((rtt_min / 1000.0) * 100.0 + 150.0))
                    OR
                    (tcp_rtt > 0 AND (CAST(rtt_min AS DOUBLE) / tcp_rtt) < 0.1)
                )
                """
                parts.append(f"NOT ({cond})" if mode == "exclude" else cond)
        elif is_signals_individual:
            for v in non_none:
                parts.append(f"{not_prefix}list_contains(string_split({sql_col}, ','), {_add_param(str(v).strip())})")
        else:
            if non_none:
                exact_vals = [v for v in non_none if not (isinstance(v, str) and "*" in v)]
                wildcard_vals = [v for v in non_none if isinstance(v, str) and "*" in v]
                sub_parts = []
                from backend.core import field_registry

                field = field_registry.try_get(clean_col)
                is_varchar = field is not None and field.duck_type == field_registry.DuckType.VARCHAR
                col_varchar = clean_col if is_varchar else f"CAST({clean_col} AS VARCHAR)"
                if exact_vals:
                    placeholders = ", ".join(_add_param(str(v)) for v in exact_vals)
                    lhs = (
                        f"CAST(CAST(ROUND({clean_col}) AS INTEGER) AS VARCHAR)"
                        if not is_varchar and clean_col in ("age", "ttl")
                        else col_varchar
                    )
                    sub_parts.append(f"{lhs} {not_prefix}IN ({placeholders})")
                for w in wildcard_vals:
                    w_like = w.replace("*", "%")
                    if inline_params:
                        w_like = "".join(ch for ch in w_like if 0x20 <= ord(ch) < 0x7F)
                    sub_parts.append(f"{col_varchar} {not_prefix}ILIKE {_add_param(w_like)}")
                if sub_parts:
                    parts.append(f"({(' AND ' if mode == 'exclude' else ' OR ').join(sub_parts)})")
            if none_vals:
                parts.append(f"{clean_col} {'IS NOT NULL' if mode == 'exclude' else 'IS NULL'}")
        if parts:
            joiner = " AND " if (is_signals_individual or mode == "exclude") else " OR "
            conditions.append(f"({joiner.join(parts)})")
    return params, (" AND ".join(conditions) if conditions else "1=1")


def build_geo_select_clause(actual_cols: list[str]) -> tuple[str, str, str, str]:
    loc_cols, label_expr, country_sel, region_sel, sel_cols = '"city"', '"city"', "NULL", "NULL", ['"city"']
    if actual_cols:
        if "region" in actual_cols:
            sel_cols.append('"region"')
            region_sel = '"region"'
        if "country" in actual_cols:
            sel_cols.append('"country"')
            country_sel = '"country"'
    if len(sel_cols) > 1:
        loc_cols = ", ".join(sel_cols)
        label_expr = f"concat({", ', ', ".join(sel_cols)})"
    return loc_cols, label_expr, country_sel, region_sel


def calc_offset(page: int, limit: int) -> int:
    return (max(1, page) - 1) * limit
