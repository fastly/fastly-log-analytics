"""SQL WHERE clause and filter builder logic."""

from __future__ import annotations

import re
from typing import Any

from backend.models.common import FiltersDict

_SAFE_COL_RE = re.compile(r"[^\w]")


from backend.utils.date_utils import parse_iso_utc


def _lookup_ngwaf_waf_req_ids(db_path: str, bot_names: list[str]) -> list[str]:
    """Return cached ``waf_req_id``s whose ``bot_name`` is in ``bot_names``.

    Reads ``ngwaf_bot_cache.db`` via plain ``sqlite3`` instead of DuckDB's
    ``sqlite_scan`` — see the ``_ngwaf_bot_name`` branch of
    :func:`build_where_clause` for why. Best-effort: returns ``[]`` on any
    sqlite error so a cache hiccup degrades the filter to "no matches"
    rather than failing the whole query.
    """
    import logging
    import sqlite3

    try:
        con = sqlite3.connect(db_path, timeout=5)
        try:
            placeholders = ", ".join("?" for _ in bot_names)
            rows = con.execute(
                f"SELECT waf_req_id FROM ngwaf_bots WHERE bot_name IN ({placeholders})",
                bot_names,
            ).fetchall()
        finally:
            con.close()
    except sqlite3.Error as e:
        logging.getLogger(__name__).warning("[build_where_clause] ngwaf_bots cache lookup failed: %s", e)
        return []
    return [r[0] for r in rows]


def filter_spec_attr(spec: Any, attr: str) -> Any:
    """Read ``attr`` from a filter spec that may be a FilterSpec object or a
    plain dict.

    Filters arrive as FilterSpec objects (attribute access) from request
    models, OR as plain dicts from tests and internal callers.
    ``getattr(some_dict, "values", None)`` returns the bound dict ``.values``
    METHOD, not the "values" key — which raised
    ``TypeError: 'builtin_function_or_method' object is not iterable`` the
    moment a non-Pydantic filter reached a cache-key serializer. Use this
    accessor to read either shape uniformly.
    """
    return spec.get(attr) if isinstance(spec, dict) else getattr(spec, attr, None)


def normalize_filter_key(filter_key: str) -> str:
    """Reduce a raw filter key to its underlying column name.

    Strips the ``filter_`` / ``xfilter_`` prefixes and the ``_<n>`` dedup
    suffix that the frontend ``buildFiltersPayload`` appends when the same
    column needs both include + exclude buckets. This MUST stay in lockstep
    with the key handling in :func:`build_where_clause` (which calls this) and
    with the analyst IP-filter lock in ``backend.utils.remote_access`` — both
    compare the normalized key against a forbidden-column set, so any drift
    would open a bypass.

    The frontend ``filterStore.addFilter`` guard rejects column names matching
    ``/_\\d+$/`` at entry, so a real field whose name ends in ``_<digit>``
    cannot reach this strip and be corrupted — any future field naming
    convention must preserve that constraint or this regex needs to change.

    Security: the final ``_SAFE_COL_RE`` strip + ``.lower()`` make this resolve
    to the SAME column :func:`build_where_clause` targets (it applies
    ``_SAFE_COL_RE.sub`` to the result below) AND to the SAME identifier DuckDB
    binds (identifier matching is case-INSENSITIVE, even when quoted). The
    analyst PII filter-lock in ``backend.utils.remote_access`` compares this
    against a lowercase forbidden-column set, so WITHOUT both steps a
    junk-suffixed OR case-variant key (``ip.``, ``ip``+space, ``IP``,
    ``Cookie_Session``) would slip the lock yet still bind the real PII column
    in SQL — a masking bypass (adversarial audit 2026-07-06). Capture columns
    are lowercase word-only names, so both steps are a no-op for legit keys
    (e.g. ``waf_sig_ind``, ``_bot_name`` are preserved).
    """
    col = filter_key
    for prefix in ("xfilter_", "filter_"):
        if col.startswith(prefix):
            col = col[len(prefix) :]
            break
    col = re.sub(r"_\d+$", "", col)
    return _SAFE_COL_RE.sub("", col).lower()


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
        col = normalize_filter_key(filter_key)

        clean_col = _SAFE_COL_RE.sub("", col)  # strip anything non-word
        is_signals_individual = col == "waf_sig_ind"
        is_bot_name = col == "_bot_name"
        is_ngwaf_bot_name = col == "_ngwaf_bot_name"
        real_col = "waf_sig" if is_signals_individual else clean_col
        sql_col = real_col
        sql_clean_col = clean_col

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
            # Virtual filter over the local NGWAF bot cache. Resolved via a
            # direct sqlite3 lookup (see _lookup_ngwaf_waf_req_ids) rather
            # than DuckDB's sqlite_scan — sqlite_scan doesn't reliably
            # coordinate with SQLite's own WAL/locking protocol, and reading
            # ngwaf_bot_cache.db through it while the sync job concurrently
            # writes the same (globally shared) file corrupted it in
            # production (2026-07-30). Skipped if waf_req_id isn't in schema.
            if actual_cols is not None and "waf_req_id" not in actual_cols:
                import logging

                logging.getLogger(__name__).warning(
                    "[build_where_clause] _ngwaf_bot_name filter skipped: 'waf_req_id' not in schema"
                )
                continue

            import os

            from backend import config as svcconfig

            ngwaf_db = svcconfig.ngwaf_db_path()
            if ngwaf_db and os.path.exists(ngwaf_db):
                bot_names = [str(v) for v in non_none if v]
                if bot_names:
                    waf_req_ids = _lookup_ngwaf_waf_req_ids(ngwaf_db, bot_names)
                    op = "NOT IN" if mode == "exclude" else "IN"
                    if waf_req_ids:
                        placeholders = ", ".join(_add_param(v) for v in waf_req_ids)
                        parts.append(f"waf_req_id {op} ({placeholders})")
                    elif mode != "exclude":
                        # No cached rows match these bot names — an include
                        # filter must match nothing (mirrors the empty-subquery
                        # IN-clause behavior this replaces). An exclude filter
                        # over an empty set is vacuously true, so no condition.
                        parts.append("FALSE")
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
