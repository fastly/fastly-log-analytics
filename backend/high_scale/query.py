"""Lossless source-object decoder and bounded console SQL execution for high-scale."""

from __future__ import annotations

import base64
import gzip
import json
import math
import re
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, urlparse

import duckdb

from backend.core.log_fields import DuckType, try_get
from backend.core.share_db import IP_FAMILY_KEYS, SESSION_ID_KEYS, mask_ip_values
from backend.core.sql_validator import SAFE_IDENT_RE, SQLValidationError, validate_user_sql
from backend.high_scale._common import _FIELD_DIMENSIONS
from backend.high_scale._common import range_value as _range_value
from backend.high_scale.archive import (
    ArchiveSourceObject,
    build_event_id,
    build_request_event_id,
    build_rum_event_id,
    row_matches_serving_domain,
)
from backend.models.analytics import QueryRequest as DashboardQueryRequest
from backend.repositories._base import SectionTimer, _compact_sql_for_debug, _safe_table
from backend.repositories.query import MAX_QUERY_ROWS
from backend.repositories.utils.response_shaping import build_query_response_envelope
from backend.utils.telemetry import get_tracked_calls

if TYPE_CHECKING:
    from backend.high_scale.ingest_controller import HighScaleService


# ── Lossless Source-Object Decoder (folded from decoder.py) ───────────────────
@dataclass(frozen=True)
class DeadLetterRecord:
    source_object_key: str
    line_ordinal: int
    raw_line: bytes
    reason: str

    @property
    def raw_line_base64(self) -> str:
        return base64.b64encode(self.raw_line).decode("ascii")


@dataclass(frozen=True)
class DecodeResult:
    source: ArchiveSourceObject
    events: tuple[dict[str, Any], ...]
    dead_letters: tuple[DeadLetterRecord, ...]
    decoded_rows: int
    accepted_rows: int
    quarantined_rows: int


def decode_source_object(
    source: ArchiveSourceObject, payload: bytes, *, transform_version: str, domain: str | None = None
) -> DecodeResult:
    selected_domain = domain or source.domain
    if selected_domain not in {"request", "rum_vitals", "rum_errors"}:
        raise ValueError("source decoder supports request, rum_vitals, and rum_errors domains")
    raw_payload = _decode_payload(payload)
    events: list[dict[str, Any]] = []
    dead_letters: list[DeadLetterRecord] = []
    source_version = source.version or source.checksum
    lines = raw_payload.splitlines()
    for line_ordinal, raw_line in enumerate(lines):
        if not raw_line.strip():
            continue
        try:
            value = json.loads(raw_line)
            if not isinstance(value, dict):
                raise ValueError("record is not a JSON object")
            raw_json_str = raw_line.decode("utf-8")
            base_meta = {
                "service_id": source.service_id,
                "domain": selected_domain,
                "source_object_key": source.object_key,
                "source_object_version": source_version,
                "line_ordinal": line_ordinal,
                "transform_version": transform_version,
                "raw_json": raw_json_str,
            }
            expanded_faro = (
                _expand_faro_rum_events(value, selected_domain)
                if selected_domain in {"rum_vitals", "rum_errors"}
                else []
            )
            if expanded_faro:
                for sub_ordinal, faro_fields in enumerate(expanded_faro):
                    event = dict(value)
                    event.update(faro_fields)
                    event.update(base_meta)
                    _normalize_serving_fields(event, selected_domain)
                    event["event_id"] = _event_id(event, selected_domain, sub_ordinal=sub_ordinal)
                    events.append(event)
            else:
                event = dict(value)
                event.update(base_meta)
                _normalize_serving_fields(event, selected_domain)
                if selected_domain in {"rum_vitals", "rum_errors"} and not row_matches_serving_domain(
                    selected_domain, event
                ):
                    sibling = "rum_errors" if selected_domain == "rum_vitals" else "rum_vitals"
                    other = dict(value)
                    _normalize_serving_fields(other, sibling)
                    if not row_matches_serving_domain(sibling, other):
                        raise ValueError("RUM record has neither a valid metric nor an error discriminator")
                event["event_id"] = _event_id(event, selected_domain)
                events.append(event)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError) as exc:
            dead_letters.append(DeadLetterRecord(source.object_key, line_ordinal, raw_line, str(exc)))
    return DecodeResult(
        source=source,
        events=tuple(events),
        dead_letters=tuple(dead_letters),
        decoded_rows=len(lines),
        accepted_rows=len(events),
        quarantined_rows=len(dead_letters),
    )


def _decode_payload(payload: bytes) -> bytes:
    if payload[:2] == b"\x1f\x8b":
        try:
            return gzip.decompress(payload)
        except (OSError, EOFError) as exc:
            raise ValueError("source gzip payload is invalid") from exc
    return payload


_FIELD_ALIASES: dict[str, str] = {
    "resp_body_size": "resp_bytes",
    "req_body_size": "req_size",
    "response_bytes": "resp_bytes",
    "status_code": "status",
    "client_ip": "ip",
}


def _normalize_serving_fields(event: dict[str, Any], domain: str) -> None:
    if domain == "request":
        custom_fields = event.setdefault("custom_fields", {})
        cmcd = event.setdefault("cmcd", {})
        keys_to_check = [
            k
            for k in list(event.keys())
            if k
            not in {
                "service_id",
                "domain",
                "source_object_key",
                "source_object_version",
                "line_ordinal",
                "transform_version",
                "raw_json",
                "event_id",
                "custom_fields",
                "cmcd",
            }
        ]
        for k in keys_to_check:
            val = event[k]
            if k.startswith("cmcd_") or k.startswith("cmcd."):
                cmcd[k] = str(val) if val is not None else ""
                short_k = k[5:]
                if short_k:
                    cmcd[short_k] = str(val) if val is not None else ""
                continue
            field = try_get(k) or try_get(_FIELD_ALIASES.get(k, ""))
            if field is not None:
                if field.duck_type in (
                    DuckType.UTINYINT,
                    DuckType.USMALLINT,
                    DuckType.UINTEGER,
                    DuckType.UBIGINT,
                    DuckType.BIGINT,
                    DuckType.INTEGER,
                ):
                    if val not in (None, ""):
                        try:
                            event[k] = int(val)
                        except (ValueError, TypeError):
                            pass
                elif field.duck_type in (DuckType.FLOAT, DuckType.DOUBLE):
                    if val not in (None, ""):
                        try:
                            event[k] = float(val)
                        except (ValueError, TypeError):
                            pass
                elif field.duck_type == DuckType.BOOLEAN:
                    if isinstance(val, str):
                        event[k] = val.lower() in ("true", "1", "t", "yes")
                    elif val is not None:
                        event[k] = bool(val)
                elif field.duck_type == DuckType.VARCHAR:
                    if val is not None and not isinstance(val, str):
                        event[k] = str(val)
            else:
                if k in {"timestamp", "time_stamp", "client_ip", "ip", "url", "fastly_pop"}:
                    continue
                custom_fields[k] = str(val) if val is not None else ""
    elif domain == "rum_vitals":
        client_id = event.get("rum_cid") or event.get("client_id") or ""
        metric_name = event.get("rum_metric_name") or event.get("metric_name") or ""
        raw_metric_value = event.get("rum_metric_value", event.get("metric_value"))
        metric_rating = event.get("rum_metric_rating") or event.get("metric_rating") or ""
        pathname = event.get("rum_pathname") or event.get("pathname") or ""
        if not metric_name or raw_metric_value in (None, ""):
            qparams = _rum_querystring_params(event)
            if qparams:
                metric_name = metric_name or _first_qparam(qparams, "rum_metric_name")
                if raw_metric_value in (None, ""):
                    raw_metric_value = _first_qparam(qparams, "rum_metric_value")
                metric_rating = metric_rating or _first_qparam(qparams, "rum_metric_rating")
                client_id = client_id or _first_qparam(qparams, "cid")
                pathname = pathname or _first_qparam(qparams, "rum_pathname")
        event["client_id"] = client_id
        event["metric_name"] = metric_name
        if raw_metric_value in (None, ""):
            event["metric_value"] = None
        elif isinstance(raw_metric_value, (int, float)) and not isinstance(raw_metric_value, bool):
            event["metric_value"] = float(raw_metric_value)
        elif isinstance(raw_metric_value, str):
            event["metric_value"] = float(raw_metric_value)
        else:
            event["metric_value"] = None
        event["metric_rating"] = metric_rating
        event["pathname"] = pathname
    elif domain == "rum_errors":
        event["client_id"] = event.get("rum_cid") or event.get("client_id") or ""
        event["error_message"] = event.get("rum_error_message") or event.get("error_message") or ""
        if not event["error_message"] and (
            event.get("_rum_kind") == "errors"
            or event.get("rum_metric_name", event.get("metric_name")) == "exception"
            or event.get("rum_error_stack")
            or event.get("error_stack")
            or event.get("rum_error_file")
            or event.get("error_file")
        ):
            event["error_message"] = "Unknown error"
        event["error_file"] = event.get("rum_error_file") or event.get("error_file") or ""
        event["pathname"] = event.get("rum_pathname") or event.get("pathname") or ""
        raw_line = event.get("rum_error_line", event.get("error_line"))
        if raw_line is not None and raw_line != "":
            try:
                event["error_line"] = int(raw_line)
            except (ValueError, TypeError):
                pass
        raw_col = event.get("rum_error_col", event.get("error_col"))
        if raw_col is not None and raw_col != "":
            try:
                event["error_col"] = int(raw_col)
            except (ValueError, TypeError):
                pass


def _is_faro_exception_metric(metric: dict[str, Any]) -> bool:
    return (metric.get("metric_name") == "exception") or bool(metric.get("error_message"))


def _expand_faro_rum_events(log_data: dict[str, Any], domain: str) -> list[dict[str, Any]]:
    raw_body = log_data.get("rum_body")
    if not raw_body:
        return []
    from backend.core.rum_ingest import extract_metrics_from_faro_payload

    try:
        payload = json.loads(raw_body) if isinstance(raw_body, str) else raw_body
        if isinstance(payload, str):
            payload = json.loads(payload)
        if not isinstance(payload, dict):
            raise ValueError("Faro RUM body must be an object")
        extracted = extract_metrics_from_faro_payload(payload, log_data)
    except (ValueError, TypeError, AttributeError) as exc:
        raise ValueError("invalid Faro RUM body") from exc
    if not extracted:
        return []
    if domain == "rum_vitals":
        domain_items = [m for m in extracted if not _is_faro_exception_metric(m)]
    else:
        domain_items = [m for m in extracted if _is_faro_exception_metric(m)]
    selected_items = domain_items if domain_items else extracted
    rows: list[dict[str, Any]] = []
    for metric in selected_items:
        is_exc = _is_faro_exception_metric(metric)
        browser_val = metric.get("browser") or log_data.get("browser") or "Chrome"
        os_val = metric.get("os") or log_data.get("os") or "macOS"
        device_val = metric.get("device") or log_data.get("device") or "Desktop"
        cid_val = metric.get("cid") or log_data.get("rum_cid") or log_data.get("cid") or ""
        pathname_val = metric.get("pathname") or log_data.get("rum_pathname") or "/"
        base_fields: dict[str, Any] = {
            "_rum_kind": "errors" if is_exc else "vitals",
            "client_id": cid_val,
            "rum_cid": cid_val,
            "pathname": pathname_val,
            "rum_pathname": pathname_val,
            "browser": browser_val,
            "os": os_val,
            "device": device_val,
        }
        if is_exc:
            raw_line = metric.get("error_line")
            raw_col = metric.get("error_col")
            try:
                err_line = int(raw_line) if raw_line is not None and raw_line != "" else 0
            except (ValueError, TypeError):
                err_line = 0
            try:
                err_col = int(raw_col) if raw_col is not None and raw_col != "" else 0
            except (ValueError, TypeError):
                err_col = 0
            err_msg = metric.get("error_message") or "Unknown error"
            if not isinstance(err_msg, str):
                raise ValueError("Faro error_message must be text")
            err_file = metric.get("error_file") or "unknown.js"
            base_fields.update(
                {
                    "error_message": err_msg,
                    "rum_error_message": err_msg,
                    "error_file": err_file,
                    "rum_error_file": err_file,
                    "error_line": err_line,
                    "rum_error_line": err_line,
                    "error_col": err_col,
                    "rum_error_col": err_col,
                    "metric_name": "",
                    "rum_metric_name": "",
                    "metric_value": None,
                    "rum_metric_value": None,
                    "metric_rating": "",
                    "rum_metric_rating": "",
                }
            )
        else:
            raw_val = metric.get("metric_value")
            if raw_val is None or raw_val == "" or isinstance(raw_val, bool):
                raise ValueError("Faro metric_value must be finite")
            val = float(raw_val)
            if not math.isfinite(val):
                raise ValueError("Faro metric_value must be finite")
            m_name = metric.get("metric_name") or "unknown"
            m_rating = metric.get("metric_rating") or ""
            base_fields.update(
                {
                    "metric_name": m_name,
                    "rum_metric_name": m_name,
                    "metric_value": val,
                    "rum_metric_value": val,
                    "metric_rating": m_rating,
                    "rum_metric_rating": m_rating,
                    "error_message": "",
                    "rum_error_message": "",
                }
            )
        rows.append(base_fields)
    return rows


def _rum_querystring_params(event: dict[str, Any]) -> dict[str, list[str]]:
    raw_url = event.get("url") or event.get("rum_raw_query") or ""
    if not raw_url:
        return {}
    try:
        return parse_qs(urlparse(raw_url).query)
    except ValueError:
        return {}


def _first_qparam(qparams: dict[str, list[str]], key: str) -> str:
    values = qparams.get(key)
    return values[0] if values else ""


def _event_id(event: dict[str, Any], domain: str, *, sub_ordinal: int | None = None) -> str:
    if domain == "request":
        return build_request_event_id(event)
    if domain == "rum_vitals":
        return build_rum_event_id(event, rum_kind="vitals", sub_ordinal=sub_ordinal)
    if domain == "rum_errors":
        return build_rum_event_id(event, rum_kind="errors", sub_ordinal=sub_ordinal)
    return build_event_id(event, domain=domain)


# ── SQL Console Query Endpoint (folded from query.py) ─────────────────────────
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
    """``additional_table_filters`` value for the fact tables the query reads, or None when nothing applies."""
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
    """Fail closed unless this ClickHouse honours ``additional_table_filters``."""
    if getattr(client, "_table_filters_verified", False):
        return
    rows = client.execute(
        "SELECT count() AS n FROM system.one", settings={"additional_table_filters": "{'system.one': 'dummy = 1'}"}
    )
    if not rows or int(rows[0].get("n", 1)) != 0:
        raise RuntimeError("the SQL console is disabled: ClickHouse is not enforcing additional_table_filters")
    try:
        client._table_filters_verified = True
    except AttributeError:
        pass


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
_RESULT_CLAUSES = re.compile(
    r"\bSETTINGS\b|\bFORMAT\b(?!\s*\()|\bINTO\s+OUTFILE\b|\bINFILE\b|\bRECURSIVE\b", re.IGNORECASE
)


def _reject_ambiguous_syntax(sql: str, table_names: frozenset[str]) -> None:
    """Reject text DuckDB (which validates the SQL) and ClickHouse (which runs it) could tokenise differently."""
    if "\\" in sql:
        raise PermissionError("backslashes are not allowed in console queries")
    for token in _QUOTE_SCAN.finditer(sql):
        if token.group(0) == '"':
            raise PermissionError("double-quoted identifiers are not allowed in console queries")
    code = _STRING_LITERAL.sub(" ", sql)
    if "'" in code:
        raise PermissionError("unterminated string literal")
    names = "|".join(sorted(re.escape(name) for name in table_names | frozenset(_FACT_DOMAINS)))
    for pat, msg, flags in (
        (_COMMENT_OR_ESCAPE, "comments, '$' and backticks are not allowed in console queries", 0),
        (r"[^\x20-\x7e\n\r\t]", "non-ASCII characters are only allowed inside string literals", 0),
        (_RESULT_CLAUSES, "SETTINGS, FORMAT, INTO OUTFILE and RECURSIVE are not allowed", 0),
        (rf"\.\s*(?:{names})\b", "schema-qualified table names are not allowed", re.IGNORECASE),
    ):
        if re.search(pat, code, flags=flags):
            raise PermissionError(msg)


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
    req: DashboardQueryRequest,
    start_time: str | None,
    end_time: str | None,
    mask_ips: bool,
    *,
    session_id: str | None = None,
    client: Any | None = None,
    tables: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Run a console query."""
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
    for pat, repl in (
        *([(rf"\b{re.escape(svc_table)}\b", names["request_facts"])] if svc_table != "logs" else []),
        (r"\blogs\b", names["request_facts"]),
        (r"\bclient_vitals\b", names["rum_vitals_facts"]),
        (r"\bclient_errors\b", names["rum_error_facts"]),
        (r"(?<!AS\s)(?<!')\btimestamp\b(?!')", "event_timestamp"),
        (r"(?<!AS\s)(?<!')\bip\b(?!')", "client_ip"),
    ):
        sql = re.sub(pat, repl, sql, flags=re.IGNORECASE)

    def _normalize_iso_literal(match: re.Match[str]) -> str:
        raw = match.group(1)
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(UTC)
            return f"'{dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"
        except ValueError:
            return match.group(0)

    sql = re.sub(
        r"'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?)'", _normalize_iso_literal, sql
    )
    for field in _FIELD_DIMENSIONS.values():
        if field in {"client_ip", "country", "url", "cmcd", "custom_fields"}:
            continue
        sql = re.sub(rf"(?<!')\b{field}\b(?!')", f"custom_fields['{field}']", sql)
    for b_field in ("edge", "is_ipv6", "waf"):
        for pat, repl in (
            (r"IN\s*\(\s*'true'\s*\)", "IN ('true', '1')"),
            (r"NOT\s+IN\s*\(\s*'true'\s*\)", "NOT IN ('true', '1')"),
            (r"IN\s*\(\s*'false'\s*\)", "IN ('false', '0', '')"),
            (r"=\s*'true'", "IN ('true', '1')"),
            (r"=\s*'false'", "IN ('false', '0', '')"),
        ):
            sql = re.sub(
                rf"custom_fields\['{b_field}'\]\s*{pat}",
                f"lower(custom_fields['{b_field}']) {repl}",
                sql,
                flags=re.IGNORECASE,
            )
    for pat, repl in (
        (r"(custom_fields\['[a-zA-Z0-9_]+'\])\s*(=|!=|<>)\s*(-?\d+(?:\.\d+)?)\b", r"\1 \2 '\3'"),
        (r"(custom_fields\['[a-zA-Z0-9_]+'\])\s*(<=|>=|<|>)\s*(-?\d+(?:\.\d+)?)\b", r"toFloat64OrZero(\1) \2 \3"),
    ):
        sql = re.sub(pat, repl, sql)
    fact_tables = {
        names[canonical]: domain
        for canonical, domain in _FACT_DOMAINS.items()
        if re.search(rf"\b{re.escape(names[canonical])}\b", sql, flags=re.IGNORECASE)
    }
    has_explicit_limit = bool(re.search(r"\bLIMIT\b", _code_only(sql), flags=re.IGNORECASE))
    _reject_unsafe_functions(sql)
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
    _debug_queries = [{"sql": compact_sql, "time_ms": elapsed_ms, "engine": "ClickHouse"}]
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
    return build_query_response_envelope(
        columns=cols,
        data=data,
        total_rows=total_rows,
        truncated=truncated,
        elapsed_ms=elapsed_ms,
        debug_queries=_debug_queries,
        debug_calls=get_tracked_calls(),
        section_timings=section_timings,
    )
