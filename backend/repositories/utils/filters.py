"""SQL WHERE clause and filter builder logic."""

from __future__ import annotations

import re
from typing import Any

from backend.models.common import FiltersDict

_SAFE_COL_RE = re.compile(r"[^\w]")


def resolve_col(col: str, actual_cols: list[str] | None) -> str:
    """Resolve a logical column name to the actual name present in the table.

    Falls back gracefully when actual_cols is unknown.
    """
    if actual_cols is None:
        return col
    if col in actual_cols:
        return col
    return col


from backend.utils.date_utils import parse_iso_utc


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
) -> tuple[list[Any], str]:
    """Build a parameterised WHERE clause from date range + column filters.

    Returns (params, where_sql) where params is the list of positional
    parameters to pass to DuckDB's execute().

    Handles:
    - Hive partition pruning via dt/hour virtual columns
    - waf_sig_ind → list_contains logic
    - include/exclude multi-value filters
    - NULL handling
    """
    conditions: list[str] = []
    params: list[Any] = []

    def _add_param(val: Any) -> str:
        if inline_params:
            if isinstance(val, str):
                # Strip NULL bytes and control characters before inlining —
                # DuckDB's SQL parser treats embedded NULL as a string
                # terminator and raises "unterminated quoted string".
                # (Discovered via hypothesis in tests/repositories/test_filters_properties.py)
                cleaned = "".join(ch for ch in val if ch == "\t" or ch == "\n" or ord(ch) >= 0x20)
                return f"'{cleaned.replace('`', '').replace(chr(39), chr(39) + chr(39))}'"
            return str(val)
        params.append(val)
        return "?"

    if start_time:
        conditions.append(f"timestamp >= CAST({_add_param(start_time)} AS TIMESTAMPTZ)")
        if partition_pruning:
            start_date = _get_utc_date_str(start_time)
            if len(start_date) == 10 and (actual_cols is None or "dt" in actual_cols):
                conditions.append(f"dt >= {_add_param(start_date)}")

            start_hour = _get_utc_hour_str(start_time)
            if len(start_hour) == 13 and (actual_cols is None or "timestamp_hour" in actual_cols):
                conditions.append(f"timestamp_hour >= {_add_param(start_hour)}")

    if end_time:
        conditions.append(f"timestamp <= CAST({_add_param(end_time)} AS TIMESTAMPTZ)")
        if partition_pruning:
            end_date = _get_utc_date_str(end_time)
            if len(end_date) == 10 and (actual_cols is None or "dt" in actual_cols):
                conditions.append(f"dt <= {_add_param(end_date)}")

            end_hour = _get_utc_hour_str(end_time)
            if len(end_hour) == 13 and (actual_cols is None or "timestamp_hour" in actual_cols):
                conditions.append(f"timestamp_hour <= {_add_param(end_hour)}")

    for filter_key, spec in filters.items():
        # Strip filter_ / xfilter_ prefixes and the `_<n>` dedup suffix that
        # frontend buildFiltersPayload appends when the same column needs
        # both include + exclude buckets. The frontend filterStore.addFilter
        # guard rejects column names matching /_\d+$/ at entry, so a real
        # field whose name ends in `_<digit>` cannot reach this strip and
        # be corrupted — any future field naming convention must preserve
        # that constraint or this regex needs to change.
        col = filter_key
        for prefix in ("xfilter_", "filter_"):
            if col.startswith(prefix):
                col = col[len(prefix) :]
                break
        col = re.sub(r"_\d+$", "", col)

        clean_col = _SAFE_COL_RE.sub("", col)  # strip anything non-word
        is_signals_individual = col == "waf_sig_ind"
        is_bot_name = col == "_bot_name"
        is_ngwaf_bot_name = col == "_ngwaf_bot_name"
        real_col = "waf_sig" if is_signals_individual else clean_col
        sql_col = resolve_col(real_col, actual_cols)
        sql_clean_col = resolve_col(clean_col, actual_cols)

        mode = spec.mode
        values = spec.values
        if not values:
            continue

        # Strip empty / whitespace-only string values defensively. The FE
        # is supposed to drop these before POSTing, but a regression there
        # would produce `WHERE ("status" IN (''))` and crash on numeric
        # columns ("Could not convert string '' to INT32"). NULL filters
        # use ``values=[None]`` explicitly — those are preserved below.
        values = [v for v in values if not (isinstance(v, str) and v.strip() == "")]
        if not values:
            continue

        non_none = [v for v in values if v is not None]
        none_vals = [v for v in values if v is None]
        parts: list[str] = []

        if is_bot_name:
            # Virtual filter: expand each bot_id to its UA regex patterns.
            # Skipped gracefully when 'ua' is not present in the source schema.
            if actual_cols is not None and "ua" not in actual_cols:
                import logging

                logging.getLogger(__name__).warning("[build_where_clause] _bot_name filter skipped: 'ua' not in schema")
                continue
            try:
                from backend.utils.bot_sources import get_bot_by_id
            except ImportError:
                continue
            for bot_id in non_none:
                bot = get_bot_by_id(str(bot_id))
                if bot is None:
                    continue
                raw_patterns = bot.get("pattern", {}).get("accepted", [])
                if not raw_patterns:
                    continue

                # Combine all patterns for this bot into a single case-insensitive regex.
                # Regex alternation in RE2 is significantly faster than multiple ILIKE checks.
                combined_regex = "|".join(raw_patterns)
                safe_regex = combined_regex.replace("'", "''")

                if mode == "exclude":
                    parts.append(f"NOT regexp_matches(ua, '(?i){safe_regex}')")
                else:
                    parts.append(f"regexp_matches(ua, '(?i){safe_regex}')")
        elif is_ngwaf_bot_name:
            # Virtual filter using DuckDB sqlite_scan on the local bot cache.
            # Skipped if waf_req_id isn't in schema.
            if actual_cols is not None and "waf_req_id" not in actual_cols:
                import logging

                logging.getLogger(__name__).warning(
                    "[build_where_clause] _ngwaf_bot_name filter skipped: 'waf_req_id' not in schema"
                )
                continue

            import os

            from backend import config as svcconfig

            ngwaf_db = svcconfig.ngwaf_db_path()
            if os.path.exists(ngwaf_db):
                ngwaf_db_escaped = ngwaf_db.replace("'", "''")
                # Group exact names
                bot_names = [str(v) for v in non_none if v]
                if bot_names:
                    placeholders = ", ".join(_add_param(v) for v in bot_names)
                    op = "NOT IN" if mode == "exclude" else "IN"
                    # We ensure waf_req_id is not null and exists in the subquery
                    parts.append(
                        f"waf_req_id {op} (SELECT waf_req_id FROM sqlite_scan('{ngwaf_db_escaped}', 'ngwaf_bots') WHERE bot_name IN ({placeholders}))"
                    )
        elif is_signals_individual:
            for v in non_none:
                val_str = str(v).strip()
                if mode == "exclude":
                    parts.append(f"NOT list_contains(string_split({sql_col}, ','), {_add_param(val_str)})")
                else:
                    parts.append(f"list_contains(string_split({sql_col}, ','), {_add_param(val_str)})")
        else:
            if non_none:
                exact_vals = []
                wildcard_vals = []
                for v in non_none:
                    if isinstance(v, str) and "*" in v:
                        wildcard_vals.append(v)
                    else:
                        exact_vals.append(v)

                sub_parts = []
                if exact_vals:
                    # CAST(col AS VARCHAR) + stringify each param defensively
                    # so a non-numeric filter value on a numeric column
                    # (e.g. ``status="abc"``) AND the reverse — a numeric
                    # filter value on a string column (e.g. ``country=0``) —
                    # both compare as strings instead of crashing with
                    # "Could not convert string ':' to INT32". The wildcard
                    # branch below already CASTs to VARCHAR for the same
                    # reason. Both surfaced via hypothesis property tests.
                    str_vals = [str(v) for v in exact_vals]
                    placeholders = ", ".join(_add_param(v) for v in str_vals)
                    op = "NOT IN" if mode == "exclude" else "IN"
                    sub_parts.append(f"CAST({sql_clean_col} AS VARCHAR) {op} ({placeholders})")

                for w in wildcard_vals:
                    w_like = w.replace("*", "%")
                    if inline_params:
                        # DuckDB's LIKE pattern optimizer mis-handles certain
                        # byte sequences (e.g. \x7f or U+00FF) when adjacent
                        # to '%' inside a SQL literal, raising "Invalid
                        # unicode (byte sequence mismatch)". Restrict the
                        # inlined LIKE pattern to ASCII printables; non-ASCII
                        # bytes can still match via the exact-IN path above.
                        # Discovered via hypothesis property tests.
                        w_like = "".join(ch for ch in w_like if 0x20 <= ord(ch) < 0x7F)
                    op = "NOT LIKE" if mode == "exclude" else "LIKE"
                    sub_parts.append(f"CAST({sql_clean_col} AS VARCHAR) {op} {_add_param(w_like)}")

                if sub_parts:
                    if mode == "exclude":
                        combined = " AND ".join(sub_parts)
                    else:
                        combined = " OR ".join(sub_parts)
                    parts.append(f"({combined})")

            if none_vals:
                null_op = "IS NOT NULL" if mode == "exclude" else "IS NULL"
                parts.append(f"{sql_clean_col} {null_op}")

        if parts:
            if is_signals_individual:
                joiner = " AND "
            elif is_bot_name or is_ngwaf_bot_name:
                # Multiple bot_ids: OR together (include) or AND NOT (exclude)
                joiner = " AND " if mode == "exclude" else " OR "
            else:
                joiner = " AND " if mode == "exclude" else " OR "
            conditions.append(f"({joiner.join(parts)})")

    where_sql = " AND ".join(conditions) if conditions else "1=1"
    return params, where_sql


def build_geo_select_clause(actual_cols: list[str]) -> tuple[str, str, str, str]:
    """
    Returns (loc_cols, label_expr, country_sel, region_sel) for reliable geo-labeling.
    Prevents duplicating COALESCE/CONCAT logic across multiple repo files.

    ``region_sel`` / ``country_sel`` are bare expressions (``"region"`` /
    ``NULL``) — callers add ``AS region`` / ``AS country`` themselves where
    needed. Returning ``NULL AS region`` here meant templates using
    ``{region_sel} AS region`` produced ``NULL AS region AS region``
    (binder error), and templates using ``{region_sel}`` in a GROUP BY
    produced ``GROUP BY ..., NULL AS region`` (also a binder error).
    """
    loc_cols = '"city"'
    label_expr = '"city"'
    country_sel, region_sel = "NULL", "NULL"
    sel_cols = ['"city"']

    if actual_cols:
        if "region" in actual_cols:
            sel_cols.append('"region"')
            region_sel = '"region"'
        if "country" in actual_cols:
            sel_cols.append('"country"')
            country_sel = '"country"'

    if len(sel_cols) > 1:
        loc_cols = ", ".join(sel_cols)
        _sep = "', '"
        label_expr = f"concat({(', ' + _sep + ', ').join(sel_cols)})"

    return loc_cols, label_expr, country_sel, region_sel
