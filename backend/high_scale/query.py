from __future__ import annotations

import re
import time
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from typing import Any

import duckdb

from backend.core.share_db.validation import IP_FAMILY_KEYS, SESSION_ID_KEYS, mask_ip_values
from backend.core.sql_ident import SAFE_IDENT_RE
from backend.high_scale._projection_utils import range_value as _range_value
from backend.high_scale.registry import HighScaleService
from backend.models.dashboard import QueryRequest
from backend.repositories._base import SectionTimer, _compact_sql_for_debug, _safe_table
from backend.repositories.query import MAX_QUERY_ROWS
from backend.utils.sql_validator import SQLValidationError, validate_user_sql
from backend.utils.telemetry import get_tracked_calls

_FACT_DOMAINS = {"request_facts": "request", "rum_vitals_facts": "rum_vitals", "rum_error_facts": "rum_errors"}


def _literal(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _instant(value: datetime) -> str:
    return f"toDateTime64({_literal(value.astimezone(UTC).strftime('%Y-%m-%d %H:%M:%S.%f'))}, 6, 'UTC')"


def _table_filters(
    service_id: str,
    start_time: str | None,
    end_time: str | None,
    tables: Iterable[str] | Mapping[str, str],
    *,
    scoped_by_view: bool = False,
) -> str | None:
    """``additional_table_filters`` value for the fact tables the query reads, or None when nothing applies.

    ``tables`` maps each table name as it appears in the SQL to its publication domain (a bare iterable of
    canonical fact-table names is accepted). By default the filter pins service, visible batches and the analyst
    window. With ``scoped_by_view`` the table is a console view that already fixes service and fence in the
    database (and the identity cannot read the publication table a fence subquery would need), so only the window
    is applied. It is sent as an HTTP setting, never as SQL text, so a user comment cannot swallow it.
    """
    domains = tables if isinstance(tables, Mapping) else {table: _FACT_DOMAINS[table] for table in tables}
    start = _range_value(start_time)
    end = _range_value(end_time)
    entries = []
    for table, domain in domains.items():
        clauses: list[str] = []
        if not scoped_by_view:
            clauses += [
                f"service_id = {_literal(service_id)}",
                "publication_state = 'visible'",
                "batch_id IN (SELECT batch_id FROM high_scale_batch_publications FINAL "
                f"WHERE service_id = {_literal(service_id)} AND domain = '{domain}' AND publication_state = 'visible')",
            ]
        if start is not None:
            clauses.append(f"event_timestamp >= {_instant(start)}")
        if end is not None:
            clauses.append(f"event_timestamp <= {_instant(end)}")
        if clauses:
            entries.append(f"{_literal(table)}: {_literal(' AND '.join(clauses))}")
    return "{" + ", ".join(entries) + "}" if entries else None


def _assert_table_filters_enforced(client: Any) -> None:
    """Fail closed unless this ClickHouse honours ``additional_table_filters``.

    ``system.one`` holds one row with ``dummy = 0``; a filter of ``dummy = 1`` must therefore hide it.
    A server that silently ignored the setting would otherwise serve every tenant's rows.
    """
    if getattr(client, "_table_filters_verified", False):
        return
    rows = client.execute(
        "SELECT count() AS n FROM system.one",
        settings={"additional_table_filters": "{'system.one': 'dummy = 1'}"},
    )
    if not rows or int(rows[0].get("n", 1)) != 0:
        raise RuntimeError("the SQL console is disabled: ClickHouse is not enforcing additional_table_filters")
    try:
        client._table_filters_verified = True
    except AttributeError:
        pass  # a client that cannot carry the marker is simply re-probed, which fails closed


# ClickHouse executes whatever function the user names, including table functions that reach other hosts
# (remote, url, s3, file, ...) and ones that disclose server state (getSetting, hostName, version). The DuckDB
# parse that validates the SQL knows none of them, so only an explicit allow-list is safe. Encoders that could
# smuggle a masked column past value-shape masking (hex, base64, reinterpretAs*) are deliberately absent.
_ALLOWED_FUNCTIONS = frozenset(
    """
    count sum avg min max any anylast argmin argmax uniq uniqexact uniqcombined countif sumif avgif minif maxif
    anyif uniqif quantile quantiles quantileexact quantileexactweighted quantiletiming quantiletdigest median
    stddevpop stddevsamp varpop varsamp topk grouparray grouparrayif groupuniqarray
    if multiif coalesce ifnull nullif isnull isnotnull assumenotnull tonullable
    lower upper lowerutf8 upperutf8 length lengthutf8 char_length substring substr substringutf8 concat concatws
    replace replaceone replaceall replaceregexpone replaceregexpall position positioncaseinsensitive startswith
    endswith trim trimleft trimright ltrim rtrim splitbychar splitbystring extract extractall match like ilike
    tostring format reverse arraystringconcat
    abs round floor ceil ceiling trunc sqrt pow power exp log log2 log10 intdiv modulo greatest least sign
    cast toint8 toint16 toint32 toint64 touint8 touint16 touint32 touint64 tofloat32 tofloat64
    toint32ornull toint64ornull touint32ornull touint64ornull tofloat64ornull toint32orzero toint64orzero
    touint32orzero touint64orzero tofloat64orzero todate todatetime todatetime64 todatetimebesteffort
    parsedatetimebesteffort parsedatetime64besteffort
    tostartofminute tostartoffiveminute tostartoftenminutes tostartoffifteenminutes tostartofhour tostartofday
    tostartofweek tostartofmonth tostartofinterval date_trunc datetrunc now today tounixtimestamp fromunixtimestamp
    formatdatetime tohour tominute tosecond todayofweek todayofmonth tomonth toyear dateadd datesub datediff
    tointervalsecond tointervalminute tointervalhour tointervalday
    array arrayelement has hasany hasall empty notempty map mapkeys mapvalues mapcontains tuple
    row_number rank dense_rank lag lead
    """.split()
)
# Words that may legitimately be followed by "(" without being function calls.
_SQL_KEYWORDS = frozenset(
    "in as and or not over where from join on select with when then else case end between like ilike using "
    "partition by order group having union all distinct exists any interval limit offset on inner "
    "full cross intersect except array global prewhere final sample".split()
)
_SQL_TOKEN = re.compile(
    r"""
    '(?:[^'\\]|\\.|'')*'            # string literal
    |"(?:[^"\\]|\\.)*"\s*\(        # quoted identifier used as a function name
    |--[^\n]*                         # line comment
    |/\*.*?\*/                       # block comment
    |\b(IDENT)\s*\(                   # function-call candidate
    """.replace("IDENT", SAFE_IDENT_RE.pattern),
    re.VERBOSE | re.DOTALL,
)


_LITERAL_OR_COMMENT = re.compile(r"'(?:[^'\\]|\\.|'')*'|--[^\n]*|/\*.*?\*/", re.DOTALL)


def _code_only(sql: str) -> str:
    """``sql`` with string literals and comments blanked, so keyword checks only see real code."""
    return _LITERAL_OR_COMMENT.sub(" ", sql)


_STRING_LITERAL = re.compile(r"'(?:[^']|'')*'")
_QUOTE_SCAN = re.compile(r"""'(?:[^']|'')*'|\"""")
_COMMENT_OR_ESCAPE = re.compile(r"--|/\*|\*/|#|\$|`")
# RECURSIVE: DuckDB's table walker puts a recursive CTE's name in scope for its own anchor branch, where
# ClickHouse resolves the same name to the real (unfiltered) table of that name.
_RESULT_CLAUSES = re.compile(
    r"\bSETTINGS\b|\bFORMAT\b(?!\s*\()|\bINTO\s+OUTFILE\b|\bINFILE\b|\bRECURSIVE\b", re.IGNORECASE
)


def _reject_ambiguous_syntax(sql: str, table_names: frozenset[str]) -> None:
    """Reject text DuckDB (which validates the SQL) and ClickHouse (which runs it) could tokenise differently.

    DuckDB nests ``/* /* */ */`` and ClickHouse does not, they disagree on ``\\'`` inside strings, on ``$$``
    heredocs and on non-ASCII whitespace; a statement crafted around any of them passes the table and function
    checks as a harmless query and runs as another. Comments, backslashes, ``$``, backticks, stray quotes,
    non-ASCII outside string literals, result clauses (``SETTINGS``, ``FORMAT``, ``INTO OUTFILE``) and
    schema-qualified fact tables have no use in console queries, so they fail closed and both parsers see
    exactly the same text.
    """
    if "\\" in sql:
        raise PermissionError("backslashes are not allowed in console queries")
    # Walk left to right so a quote inside a double-quoted identifier can never be paired with a later one
    # (`"a'"` ... `"b'"` would otherwise read as a string literal and hide whatever lies between).
    for token in _QUOTE_SCAN.finditer(sql):
        if token.group(0) == '"':
            raise PermissionError("double-quoted identifiers are not allowed in console queries")
    code = _STRING_LITERAL.sub(" ", sql)
    if "'" in code:
        raise PermissionError("unterminated string literal")
    if _COMMENT_OR_ESCAPE.search(code):
        raise PermissionError("comments, '$' and backticks are not allowed in console queries")
    if re.search(r"[^\x20-\x7e\n\r\t]", code):
        raise PermissionError("non-ASCII characters are only allowed inside string literals")
    if _RESULT_CLAUSES.search(code):
        raise PermissionError("SETTINGS, FORMAT, INTO OUTFILE and RECURSIVE are not allowed")
    names = "|".join(sorted(re.escape(name) for name in table_names | frozenset(_FACT_DOMAINS)))
    if re.search(rf"\.\s*(?:{names})\b", code, flags=re.IGNORECASE):
        raise PermissionError("schema-qualified table names are not allowed")


def _reject_unsafe_functions(sql: str) -> None:
    for match in _SQL_TOKEN.finditer(sql):
        token = match.group(0)
        name = match.group(1)
        if name is None:
            if token.startswith('"'):
                raise PermissionError("quoted function names are not allowed")
            continue
        lowered = name.lower()
        if lowered in _SQL_KEYWORDS or lowered in _ALLOWED_FUNCTIONS:
            continue
        raise PermissionError(f"function {name!r} is not allowed in console queries")


def query_endpoint(
    service: HighScaleService,
    req: QueryRequest,
    start_time: str | None,
    end_time: str | None,
    mask_ips: bool,
    *,
    session_id: str | None = None,
    client: Any | None = None,
    tables: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Run a console query.

    ``client`` overrides the service's own ClickHouse client and ``tables`` maps each canonical fact table to the
    view to read it through; analysts pass both (their identity has grants on those views only).
    """
    client = client if client is not None else service.client
    names = {canonical: (tables or {}).get(canonical, canonical) for canonical in _FACT_DOMAINS}
    timer = SectionTimer()
    section_timings = timer.entries

    raw_sql = req.sql.strip()
    max_rows = max(1, min(int(req.max_rows), MAX_QUERY_ROWS))
    svc_table = _safe_table(service.service_id)
    allowed_tables = frozenset({"logs", "client_vitals", "client_errors", svc_table.lower()})

    _t_val = time.perf_counter()
    _reject_ambiguous_syntax(raw_sql, allowed_tables | frozenset(names.values()))
    with duckdb.connect(":memory:") as parser_con:
        try:
            validate_user_sql(
                raw_sql,
                parser_con=parser_con,
                session_id=session_id,
                service_id=service.service_id,
                allowed_tables=allowed_tables,
            )
        except SQLValidationError as exc:
            raise PermissionError(exc.message) from exc
    timer.mark("validate_user_sql", _t_val)

    sql = raw_sql.rstrip().rstrip(";")

    # Translate DuckDB table names to ClickHouse fact tables
    if svc_table != "logs":
        sql = re.sub(rf"\b{re.escape(svc_table)}\b", names["request_facts"], sql, flags=re.IGNORECASE)
    sql = re.sub(r"\blogs\b", names["request_facts"], sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bclient_vitals\b", names["rum_vitals_facts"], sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bclient_errors\b", names["rum_error_facts"], sql, flags=re.IGNORECASE)

    # Translate canonical column names for ClickHouse fact tables
    sql = re.sub(r"(?<!AS\s)(?<!')\btimestamp\b(?!')", "event_timestamp", sql, flags=re.IGNORECASE)
    sql = re.sub(r"(?<!AS\s)(?<!')\bip\b(?!')", "client_ip", sql, flags=re.IGNORECASE)

    from backend.high_scale.dashboard import _FIELD_DIMENSIONS

    for field in _FIELD_DIMENSIONS.values():
        if field in {"client_ip", "country", "url", "cmcd", "custom_fields"}:
            continue
        sql = re.sub(rf"(?<!')\b{field}\b(?!')", f"custom_fields['{field}']", sql)

    fact_tables = {
        names[canonical]: domain
        for canonical, domain in _FACT_DOMAINS.items()
        if re.search(rf"\b{re.escape(names[canonical])}\b", sql, flags=re.IGNORECASE)
    }

    has_explicit_limit = bool(re.search(r"\bLIMIT\b", _code_only(sql), flags=re.IGNORECASE))
    _reject_unsafe_functions(sql)

    # Always bound the result: wrapping puts the cap outside the user's SQL, and the newline before ``)`` stops a
    # trailing ``--`` comment from swallowing it. An explicit LIMIT may exceed max_rows (so total_rows stays
    # meaningful) but never the global ceiling.
    row_cap = (MAX_QUERY_ROWS if has_explicit_limit else max_rows) + 1
    sql = f"SELECT * FROM (\n{sql}\n) LIMIT {row_cap}"
    settings = {
        "max_result_rows": str(row_cap),
        "result_overflow_mode": "break",
        "date_time_input_format": "best_effort",
    }
    if fact_tables:
        _assert_table_filters_enforced(client)
        scope = _table_filters(service.service_id, start_time, end_time, fact_tables, scoped_by_view=tables is not None)
        if scope is not None:
            settings["additional_table_filters"] = scope

    t0 = time.perf_counter()
    try:
        res = client.execute(sql, settings=settings)
    except Exception as e:
        raise RuntimeError(str(e)) from e
    timer.mark("execute", t0)
    elapsed_ms = round((time.perf_counter() - t0) * 1000, 2)
    compact_sql = _compact_sql_for_debug(sql.strip())

    _debug_queries = [
        {
            "sql": compact_sql,
            "time_ms": elapsed_ms,
            "engine": "ClickHouse",
        }
    ]

    fetched_rows = len(res) if res else 0
    truncated = fetched_rows > max_rows
    if truncated:
        res = res[:max_rows]
    total_rows = -1 if (truncated and not has_explicit_limit) else fetched_rows

    _t_serialize = time.perf_counter()
    data: list[dict[str, Any]] = []
    cols = list(res[0].keys()) if res else []
    redact_keys = IP_FAMILY_KEYS | SESSION_ID_KEYS | {"client_ip"}

    for d in res:
        row_copy = dict(d)
        for k, v in row_copy.items():
            if hasattr(v, "isoformat"):
                val = v.isoformat()
                row_copy[k] = val.replace("+00:00", "Z") if "+" in val else val + "Z"
            elif mask_ips and k.lower() in redact_keys and v:
                row_copy[k] = "[redacted]"
        data.append(row_copy)

    if mask_ips:
        data = mask_ip_values(data)
    timer.mark("serialize_json", _t_serialize)

    return {
        "columns": cols,
        "data": data,
        "row_count": len(data),
        "total_rows": total_rows,
        "truncated": truncated,
        "elapsed_ms": int(elapsed_ms),
        "debug_queries": _debug_queries,
        "_debug_queries": _debug_queries,
        "debug_calls": get_tracked_calls(),
        "section_timings": section_timings,
    }
