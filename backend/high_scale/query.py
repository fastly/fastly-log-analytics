"""Lossless source-object decoder and bounded console SQL execution for high-scale."""

from __future__ import annotations

import base64
import gzip
import io
import json
import math
import os
import re
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, urlparse

import orjson
import zstandard as zstd

from backend.core.log_fields import SqlType, try_get
from backend.core.share_db import apply_pii_policy, is_saved_view_pii_key, mask_ip_values, scrub_ip_text
from backend.core.sql_validator import SAFE_IDENT_RE, SQLValidationError, validate_user_sql
from backend.high_scale._common import _FIELD_DIMENSIONS, clickhouse_string_literal
from backend.high_scale._common import range_value as _range_value
from backend.high_scale.archive import (
    ArchiveSourceObject,
    build_event_id,
    build_request_event_id,
    build_rum_event_id,
    row_matches_serving_domain,
)
from backend.models.analytics import QueryRequest as DashboardQueryRequest
from backend.repositories.query import MAX_QUERY_ROWS
from backend.repositories.utils.response_shaping import (
    SectionTimer,
    _compact_sql_for_debug,
    _safe_table,
    build_query_response_envelope,
)
from backend.utils.bounded_cache import BoundedTTLCache, CacheRegistry
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
            value = orjson.loads(raw_line)
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
                event = value if selected_domain == "request" else dict(value)
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
    if payload[:4] == b"\x28\xb5\x2f\xfd":
        try:
            with zstd.ZstdDecompressor().stream_reader(io.BytesIO(payload)) as reader:
                return reader.read()
        except zstd.ZstdError as exc:
            raise ValueError("source zstd payload is invalid") from exc
    return payload


_FIELD_ALIASES: dict[str, str] = {
    "resp_body_size": "resp_bytes",
    "req_body_size": "req_size",
    "response_bytes": "resp_bytes",
    "status_code": "status",
    "client_ip": "ip",
}

_SKIP_REQUEST_KEYS: frozenset[str] = frozenset(
    {
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
)
_PASSTHROUGH_NON_CATALOG_KEYS: frozenset[str] = frozenset(
    {"timestamp", "time_stamp", "client_ip", "ip", "url", "fastly_pop"}
)
_INT_SQL_TYPES: frozenset[SqlType] = frozenset(
    {
        SqlType.UTINYINT,
        SqlType.USMALLINT,
        SqlType.UINTEGER,
        SqlType.UBIGINT,
        SqlType.BIGINT,
        SqlType.INTEGER,
    }
)
_FLOAT_SQL_TYPES: frozenset[SqlType] = frozenset({SqlType.FLOAT, SqlType.DOUBLE})
_REQUEST_KEY_ACTION: dict[str, int] = {}


def _get_request_key_action(k: str) -> int:
    if k in _SKIP_REQUEST_KEYS:
        action = 0
    elif k.startswith("cmcd_") or k.startswith("cmcd."):
        action = 5
    else:
        field = try_get(k) or try_get(_FIELD_ALIASES.get(k, ""))
        if field is not None:
            if field.sql_type in _INT_SQL_TYPES:
                action = 1
            elif field.sql_type in _FLOAT_SQL_TYPES:
                action = 2
            elif field.sql_type == SqlType.BOOLEAN:
                action = 3
            elif field.sql_type == SqlType.VARCHAR:
                action = 4
            else:
                action = 0
        elif k in _PASSTHROUGH_NON_CATALOG_KEYS:
            action = 0
        else:
            action = 6
    _REQUEST_KEY_ACTION[k] = action
    return action


def _normalize_serving_fields(event: dict[str, Any], domain: str) -> None:
    if domain in {"rum_vitals", "rum_errors"}:
        event["req_id"] = (
            event.get("req_id")
            or event.get("fastly_req_id")
            or _first_qparam(_rum_querystring_params(event), "req")
            or ""
        )
    if domain == "request":
        custom_fields = event.setdefault("custom_fields", {})
        cmcd = event.setdefault("cmcd", {})
        key_actions = _REQUEST_KEY_ACTION
        for k, val in event.items():
            action = key_actions.get(k)
            if action is None:
                action = _get_request_key_action(k)
            if action == 0:
                continue
            if action == 4:
                if val is not None and type(val) is not str:
                    event[k] = str(val)
            elif action == 1:
                if type(val) is not int and val not in (None, ""):
                    try:
                        event[k] = int(val)
                    except (ValueError, TypeError):
                        pass
            elif action == 2:
                if type(val) is not float and val not in (None, ""):
                    try:
                        event[k] = float(val)
                    except (ValueError, TypeError):
                        pass
            elif action == 3:
                if type(val) is not bool:
                    if isinstance(val, str):
                        event[k] = val.lower() in ("true", "1", "t", "yes")
                    elif val is not None:
                        event[k] = bool(val)
            elif action == 5:
                s_val = str(val) if val is not None else ""
                cmcd[k] = s_val
                short_k = k[5:]
                if short_k:
                    cmcd[short_k] = s_val
            else:
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
        client_id = event.get("rum_cid") or event.get("client_id") or ""
        error_message = event.get("rum_error_message") or event.get("error_message") or ""
        error_file = event.get("rum_error_file") or event.get("error_file") or ""
        pathname = event.get("rum_pathname") or event.get("pathname") or ""
        raw_line = event.get("rum_error_line", event.get("error_line"))
        raw_col = event.get("rum_error_col", event.get("error_col"))
        if event.get("_rum_kind") != "vitals" and (
            not error_message or raw_line in (None, "") or raw_col in (None, "")
        ):
            qparams = _rum_querystring_params(event)
            if qparams:
                error_message = error_message or _first_qparam(qparams, "rum_error_message")
                error_file = error_file or _first_qparam(qparams, "rum_error_file")
                if raw_line in (None, ""):
                    raw_line = _first_qparam(qparams, "rum_error_line")
                if raw_col in (None, ""):
                    raw_col = _first_qparam(qparams, "rum_error_col")
                client_id = client_id or _first_qparam(qparams, "cid")
                pathname = pathname or _first_qparam(qparams, "rum_pathname")
        event["client_id"] = client_id
        event["error_message"] = error_message
        if not event["error_message"] and (
            event.get("_rum_kind") == "errors"
            or event.get("rum_metric_name", event.get("metric_name")) == "exception"
            or event.get("rum_error_stack")
            or event.get("error_stack")
            or error_file
        ):
            event["error_message"] = "Unknown error"
        event["error_file"] = error_file
        event["pathname"] = pathname
        if raw_line is not None and raw_line != "":
            try:
                event["error_line"] = int(raw_line)
            except (ValueError, TypeError):
                pass
        if raw_col is not None and raw_col != "":
            try:
                event["error_col"] = int(raw_col)
            except (ValueError, TypeError):
                pass


def _is_faro_exception_metric(metric: dict[str, Any]) -> bool:
    return (metric.get("metric_name") == "exception") or bool(metric.get("error_message"))


def _expand_faro_rum_events(log_data: dict[str, Any], domain: str) -> list[dict[str, Any]]:
    raw_body = log_data.get("rum_body")
    extracted: list[dict[str, Any]] = []
    if raw_body:
        from backend.core.rum_ingest import (
            extract_metrics_from_faro_payload,
            extract_metrics_from_query_params,
        )

        try:
            payload = json.loads(raw_body) if isinstance(raw_body, str) else raw_body
            if isinstance(payload, str):
                payload = json.loads(payload)
            if not isinstance(payload, dict):
                raise ValueError("Faro RUM body must be an object")
            extracted = extract_metrics_from_faro_payload(payload, log_data)
        except (ValueError, TypeError, AttributeError) as exc:
            qparams = _rum_querystring_params(log_data)
            if qparams:
                extracted = extract_metrics_from_query_params(qparams, log_data)
            if not extracted:
                has_fallback_fields = bool(
                    (log_data.get("rum_metric_name") and log_data.get("rum_metric_value") not in (None, ""))
                    or log_data.get("rum_error_message")
                    or log_data.get("rum_error_file")
                    or log_data.get("rum_error_stack")
                    or (
                        qparams
                        and (
                            (
                                _first_qparam(qparams, "rum_metric_name")
                                and _first_qparam(qparams, "rum_metric_value") not in (None, "")
                            )
                            or _first_qparam(qparams, "rum_error_message")
                            or _first_qparam(qparams, "rum_error_file")
                        )
                    )
                )
                if has_fallback_fields:
                    return []
                raise ValueError("invalid Faro RUM body") from exc
    else:
        qparams = _rum_querystring_params(log_data)
        if qparams:
            from backend.core.rum_ingest import extract_metrics_from_query_params

            extracted = extract_metrics_from_query_params(qparams, log_data)
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
                    "error_file": "",
                    "rum_error_file": "",
                    "error_line": 0,
                    "rum_error_line": 0,
                    "error_col": 0,
                    "rum_error_col": 0,
                }
            )
        rows.append(base_fields)
    return rows


def _rum_querystring_params(event: dict[str, Any]) -> dict[str, list[str]]:
    raw_url = event.get("url") or event.get("rum_raw_query") or ""
    if not raw_url:
        return {}
    try:
        return parse_qs(urlparse(raw_url).query, keep_blank_values=True)
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
    return clickhouse_string_literal(value)


def _instant(value: datetime) -> str:
    value_literal = clickhouse_string_literal(value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f"))
    return f"toDateTime64({value_literal}, 6, 'UTC')"


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
_COMMENT_OR_ESCAPE = re.compile(r"--|/\*|\*/|#|\$|`|[{}]")
_RESULT_CLAUSES = re.compile(
    r"\bSETTINGS\b|\bFORMAT\b(?!\s*\()|\bINTO\s+OUTFILE\b|\bINFILE\b|\bRECURSIVE\b"
    r"|\bTABLE\b|\bPIVOT\b|\bUNPIVOT\b|\bPIVOT_WIDER\b|\bPIVOT_LONGER\b|\bONLY\b",
    re.IGNORECASE,
)


def _reject_ambiguous_syntax(sql: str, table_names: frozenset[str]) -> None:
    """Reject text the SQL validator and ClickHouse could tokenise differently."""
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


_HS_QUERY_RESULT_CACHE: BoundedTTLCache = BoundedTTLCache(maxsize=256, ttl_seconds=15.0)
CacheRegistry.register("high_scale.query._HS_QUERY_RESULT_CACHE", _HS_QUERY_RESULT_CACHE)

_CONSOLE_SQL_LEX_RE = re.compile(
    r"(?P<COMMENT>--[^\n]*|/\*[\s\S]*?\*/)"
    r"|(?P<STR>'(?:''|[^'])*')"
    r'|(?P<DQUOTE>"(?:""|[^"])*"|`(?:``|[^`])*`)'
    r"|(?P<WS>\s+)"
    r"|(?P<NUM>-?\d+(?:_\d+)*(?:\.(?!\.)(?:\d+(?:_\d+)*)?)?(?:[eE][+-]?\d+(?:_\d+)*)?)"
    r"|(?P<IDENT>" + SAFE_IDENT_RE.pattern + ")"
    r"|(?P<OP><=|>=|!=|<>|[=<>(),.\[\]{}*+-/;])"
    r"|(?P<OTHER>.)"
)
_FROM_EXIT_KEYWORDS = frozenset(
    {
        "WHERE",
        "PREWHERE",
        "GROUP",
        "HAVING",
        "ORDER",
        "LIMIT",
        "OFFSET",
        "FETCH",
        "WINDOW",
        "QUALIFY",
        "UNION",
        "INTERSECT",
        "EXCEPT",
        "SETTINGS",
        "FORMAT",
        "INTO",
        "SET",
        "FOR",
    }
)
_JOIN_MODIFIER_KEYWORDS = frozenset(
    {
        "INNER",
        "LEFT",
        "RIGHT",
        "FULL",
        "OUTER",
        "CROSS",
        "NATURAL",
        "GLOBAL",
        "ANY",
        "ALL",
        "ASOF",
        "SEMI",
        "ANTI",
    }
)
_SQL_CLAUSE_STOPWORDS = (
    _FROM_EXIT_KEYWORDS
    | _JOIN_MODIFIER_KEYWORDS
    | frozenset(
        {
            "JOIN",
            "ON",
            "USING",
            "FINAL",
            "SAMPLE",
            "ARRAY",
            "AS",
            "SELECT",
            "FROM",
            "LATERAL",
        }
    )
)
_ON_EXPR_PREV_WORDS = frozenset(
    {
        "ON",
        "AND",
        "OR",
        "XOR",
        "NOT",
        "IS",
        "IN",
        "GLOBAL",
        "LIKE",
        "ILIKE",
        "RLIKE",
        "REGEXP",
        "GLOB",
        "SIMILAR",
        "TO",
        "ESCAPE",
        "BETWEEN",
        "SYMMETRIC",
        "ASYMMETRIC",
        "CASE",
        "WHEN",
        "THEN",
        "ELSE",
        "DISTINCT",
        "DIV",
        "MOD",
        "FROM",
        "COLLATE",
        "ZONE",
        "INTERVAL",
    }
)
_ON_EXPR_NEXT_WORDS = frozenset(
    {
        "IS",
        "IN",
        "BETWEEN",
        "LIKE",
        "ILIKE",
        "RLIKE",
        "REGEXP",
        "GLOB",
        "SIMILAR",
        "DIV",
        "MOD",
        "COLLATE",
        "WHEN",
        "THEN",
        "ELSE",
        "END",
        "AND",
        "OR",
        "XOR",
    }
)
_FROM_FIRST_SELECT_FN_KEYWORDS = frozenset({"ANY", "ARRAY", "LEFT", "RIGHT", "FORMAT", "SAMPLE", "WINDOW"})
_REQUEST_CF_FIELDS = frozenset(
    f for f in _FIELD_DIMENSIONS.values() if f not in {"client_ip", "country", "url", "cmcd", "custom_fields"}
)


def _rewrite_console_sql_with_canonicals(sql: str, *, svc_table: str, names: Mapping[str, str]) -> tuple[str, set[str]]:
    import secrets as _secrets

    iso_lit_re = re.compile(r"^'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?)'$")
    cf_key_lit_re = re.compile(r"^'[A-Za-z0-9_]+'$")

    def _normalize_iso(lit: str) -> str:
        m_iso = iso_lit_re.fullmatch(lit)
        if not m_iso:
            return lit
        raw = m_iso.group(1)
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(UTC)
            return f"'{dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"
        except ValueError:
            return lit

    table_map: dict[str, tuple[str, str, str]] = {}
    if "request_facts" in names:
        req_target = names["request_facts"]
        table_map["logs"] = (req_target, "request", "request_facts")
        table_map["request_facts"] = (req_target, "request", "request_facts")
        table_map[svc_table.lower()] = (req_target, "request", "request_facts")
    if "rum_vitals_facts" in names:
        vit_target = names["rum_vitals_facts"]
        table_map["client_vitals"] = (vit_target, "rum_vitals", "rum_vitals_facts")
        table_map["rum_page_load_facts"] = (vit_target, "rum_vitals", "rum_vitals_facts")
        table_map["rum_vitals_facts"] = (vit_target, "rum_vitals", "rum_vitals_facts")
    if "rum_error_facts" in names:
        err_target = names["rum_error_facts"]
        table_map["client_errors"] = (err_target, "rum_errors", "rum_error_facts")
        table_map["rum_error_facts"] = (err_target, "rum_errors", "rum_error_facts")
    for canonical, domain in _FACT_DOMAINS.items():
        if canonical in names:
            table_map[names[canonical].lower()] = (names[canonical], domain, canonical)

    def _sig_indices(tokens: list[tuple[str, str]]) -> list[int]:
        return [i for i, (k, _) in enumerate(tokens) if k not in ("WS", "COMMENT")]

    def _prev_sig_idx(tokens: list[tuple[str, str]], start_before: int) -> int | None:
        for p in range(start_before - 1, -1, -1):
            if tokens[p][0] not in ("WS", "COMMENT"):
                return p
        return None

    def _is_dot_or_dcolon_lex(tokens: list[tuple[str, str]], pos: int | None) -> bool:
        if pos is None or pos < 0:
            return False
        pk, pt = tokens[pos]
        if (pk, pt) == ("OP", "."):
            return True
        return (
            (pk, pt) == ("OTHER", ":")
            and pos >= 1
            and tokens[pos - 1] == ("OTHER", ":")
            and not (pos >= 2 and tokens[pos - 2] == ("OTHER", ":"))
        )

    def _is_non_kw_lex_pos(tokens: list[tuple[str, str]], idx: int) -> bool:
        p = _prev_sig_idx(tokens, idx)
        if p is None:
            return False
        if _is_dot_or_dcolon_lex(tokens, p):
            return True
        r = 0
        while p is not None and tokens[p][0] == "IDENT" and tokens[p][1].upper() == "AS":
            r += 1
            p = _prev_sig_idx(tokens, p)
        if r == 0:
            return False
        if _is_dot_or_dcolon_lex(tokens, p):
            r -= 1
        return (r % 2) == 1

    def _is_valid_lex_ident_list(tokens: list[tuple[str, str]]) -> bool:
        sig = _sig_indices(tokens)
        if not sig or len(sig) % 2 == 0:
            return False
        return all(
            (tokens[t_i][0] in ("IDENT", "DQUOTE") if pos % 2 == 0 else tokens[t_i] == ("OP", ","))
            for pos, t_i in enumerate(sig)
        )

    def _is_lex_distinct_from(tokens: list[tuple[str, str]], s_pos: int, sig_list: list[int]) -> bool:
        if s_pos < 2:
            return False
        p1 = tokens[sig_list[s_pos - 1]]
        if p1[0] != "IDENT" or p1[1].upper() != "DISTINCT":
            return False
        p2 = tokens[sig_list[s_pos - 2]]
        if p2[0] != "IDENT":
            return False
        if p2[1].upper() == "IS":
            return True
        if p2[1].upper() == "NOT" and s_pos >= 3:
            p3 = tokens[sig_list[s_pos - 3]]
            return p3[0] == "IDENT" and p3[1].upper() == "IS"
        return False

    def _is_lex_dot_qualifier(tokens: list[tuple[str, str]], s_pos: int, sig_list: list[int]) -> bool:
        return (
            s_pos + 2 < len(sig_list)
            and tokens[sig_list[s_pos + 1]] == ("OP", ".")
            and (
                tokens[sig_list[s_pos + 2]][0] in ("IDENT", "DQUOTE", "STR")
                or tokens[sig_list[s_pos + 2]] == ("OP", "*")
            )
        )

    def _is_lex_within_group(tokens: list[tuple[str, str]], s_pos: int, sig_list: list[int]) -> bool:
        if s_pos <= 0 or s_pos + 1 >= len(sig_list):
            return False
        cur_idx = sig_list[s_pos]
        prev_idx = sig_list[s_pos - 1]
        nxt_tok = tokens[sig_list[s_pos + 1]]
        return (
            tokens[cur_idx][0] == "IDENT"
            and tokens[cur_idx][1].upper() == "GROUP"
            and not _is_non_kw_lex_pos(tokens, cur_idx)
            and tokens[prev_idx][0] == "IDENT"
            and tokens[prev_idx][1].upper() == "WITHIN"
            and not _is_non_kw_lex_pos(tokens, prev_idx)
            and (nxt_tok == ("OP", "(") or nxt_tok[0] == "SUBQ")
        )

    def _is_lex_on_expr_ident(tokens: list[tuple[str, str]], s_pos: int, sig_list: list[int]) -> bool:
        if _is_lex_dot_qualifier(tokens, s_pos, sig_list):
            return True
        wu = tokens[sig_list[s_pos]][1].upper()
        prev_idx = sig_list[s_pos - 1] if s_pos > 0 else None
        prev_sig = tokens[prev_idx] if prev_idx is not None else None
        nxt_sig = tokens[sig_list[s_pos + 1]] if s_pos + 1 < len(sig_list) else None
        if prev_sig is not None:
            if prev_sig[0] == "OTHER" or (prev_sig[0] == "OP" and prev_sig[1] not in (")", "]", "}", ";")):
                return True
            if (
                prev_sig[0] == "IDENT"
                and prev_sig[1].upper() in _ON_EXPR_PREV_WORDS
                and prev_idx is not None
                and not _is_non_kw_lex_pos(tokens, prev_idx)
            ):
                return True
        if nxt_sig is not None:
            if nxt_sig[0] == "OTHER":
                if wu in ("SELECT", "WHERE", "PREWHERE", "HAVING", "QUALIFY", "LIMIT", "OFFSET") and nxt_sig[1] in (
                    "~",
                    "!",
                ):
                    return False
                return True
            if nxt_sig[0] == "OP" and nxt_sig[1] not in ("(", ")", "[", "]", "{", "}", ",", ";"):
                if wu == "SELECT" and nxt_sig[1] in ("*", "+", "-"):
                    return False
                if wu in ("WHERE", "PREWHERE", "HAVING", "QUALIFY", "LIMIT", "OFFSET") and nxt_sig[1] in ("+", "-"):
                    return False
                return True
            if (
                nxt_sig[0] == "IDENT"
                and nxt_sig[1].upper() in _ON_EXPR_NEXT_WORDS
                and wu not in ("SELECT", "WHERE", "PREWHERE", "HAVING", "QUALIFY")
            ):
                return True
        return False

    def _can_exit_from_lex(
        tokens: list[tuple[str, str]], s_pos: int, sig_list: list[int], *, in_on_clause: bool
    ) -> bool:
        if _is_lex_within_group(tokens, s_pos, sig_list):
            return False
        if not in_on_clause:
            return True
        wu = tokens[sig_list[s_pos]][1].upper()
        nxt_sig = tokens[sig_list[s_pos + 1]] if s_pos + 1 < len(sig_list) else None
        if wu in ("GROUP", "ORDER"):
            return nxt_sig is not None and nxt_sig[0] == "IDENT" and nxt_sig[1].upper() in ("BY", "ALL")
        if wu == "FETCH":
            return nxt_sig is not None and nxt_sig[0] == "IDENT" and nxt_sig[1].upper() in ("FIRST", "NEXT")
        return True

    def _find_matching_paren(tokens: list[tuple[str, str]], open_idx: int) -> int:
        depth = 0
        for idx in range(open_idx, len(tokens)):
            k, t = tokens[idx]
            if k == "OP" and t == "(":
                depth += 1
            elif k == "OP" and t == ")":
                depth -= 1
                if depth == 0:
                    return idx
        return len(tokens) - 1

    def _has_enclosing_parens(tokens: list[tuple[str, str]]) -> tuple[int, int] | None:
        sig = _sig_indices(tokens)
        if len(sig) >= 2 and tokens[sig[0]] == ("OP", "(") and _find_matching_paren(tokens, sig[0]) == sig[-1]:
            return sig[0], sig[-1]
        return None

    def _is_subquery_tokens(inner_tokens: list[tuple[str, str]]) -> bool:
        cur = inner_tokens
        while (enc := _has_enclosing_parens(cur)) is not None:
            cur = cur[enc[0] + 1 : enc[1]]
        sig = _sig_indices(cur)
        if not sig:
            return False
        k0, t0 = cur[sig[0]]
        if k0 == "IDENT" and t0.upper() in ("SELECT", "WITH", "FROM", "TABLE"):
            return True
        if k0 == "OP" and t0 == "(":
            close_first = _find_matching_paren(cur, sig[0])
            if close_first < sig[-1]:
                j = close_first + 1
                while j < len(cur) and cur[j][0] in ("WS", "COMMENT"):
                    j += 1
                if (
                    j < len(cur)
                    and cur[j][0] == "IDENT"
                    and cur[j][1].upper() in ("UNION", "INTERSECT", "EXCEPT", "ORDER", "LIMIT", "OFFSET", "FETCH")
                ):
                    return _is_subquery_tokens(cur[sig[0] + 1 : close_first])
        return False

    def _coerce_request_predicates(segment_tokens: list[tuple[str, str]]) -> str:
        lit_nonce = _secrets.token_hex(8)
        saved_lits: list[str] = []
        parts: list[str] = []
        sig_idxs = _sig_indices(segment_tokens)
        sig_pos_map = {idx: pos for pos, idx in enumerate(sig_idxs)}
        for idx, (k, t) in enumerate(segment_tokens):
            if k == "STR":
                t_norm = _normalize_iso(t)
                s_pos = sig_pos_map.get(idx, -1)
                is_cf_key = (
                    cf_key_lit_re.fullmatch(t_norm) is not None
                    and s_pos >= 2
                    and s_pos + 1 < len(sig_idxs)
                    and segment_tokens[sig_idxs[s_pos - 2]][1].lower() == "custom_fields"
                    and segment_tokens[sig_idxs[s_pos - 1]] == ("OP", "[")
                    and segment_tokens[sig_idxs[s_pos + 1]] == ("OP", "]")
                )
                if is_cf_key or t_norm.lower() in ("'true'", "'false'"):
                    parts.append(t_norm)
                else:
                    s_idx = len(saved_lits)
                    saved_lits.append(t_norm)
                    parts.append(f"__fla_str_lit_{lit_nonce}_{s_idx}__")
            else:
                parts.append(t)
        seg_sql = "".join(parts)
        for b_field in ("edge", "is_ipv6", "waf"):
            for pat, repl in (
                (r"IN\s*\(\s*'true'\s*\)", "IN ('true', '1')"),
                (r"NOT\s+IN\s*\(\s*'true'\s*\)", "NOT IN ('true', '1')"),
                (r"IN\s*\(\s*'false'\s*\)", "IN ('false', '0', '')"),
                (r"=\s*'true'", "IN ('true', '1')"),
                (r"=\s*'false'", "IN ('false', '0', '')"),
                (r"=\s*\btrue\b", "IN ('true', '1')"),
                (r"=\s*\bfalse\b", "IN ('false', '0', '')"),
            ):
                seg_sql = re.sub(
                    rf"custom_fields\['{b_field}'\]\s*{pat}",
                    f"lower(custom_fields['{b_field}']) {repl}",
                    seg_sql,
                    flags=re.IGNORECASE,
                )
        for pat, repl in (
            (r"(custom_fields\['[a-zA-Z0-9_]+'\])\s*(=|!=|<>)\s*(-?\d+(?:\.\d+)?)\b", r"\1 \2 '\3'"),
            (r"(custom_fields\['[a-zA-Z0-9_]+'\])\s*(<=|>=|<|>)\s*(-?\d+(?:\.\d+)?)\b", r"toFloat64OrZero(\1) \2 \3"),
        ):
            seg_sql = re.sub(pat, repl, seg_sql)
        if saved_lits:
            seg_sql = re.sub(rf"__fla_str_lit_{lit_nonce}_(\d+)__", lambda m: saved_lits[int(m.group(1))], seg_sql)
        return seg_sql

    def _rewrite_tokens(
        tokens: list[tuple[str, str]], inherited_ctes: dict[str, set[str]]
    ) -> tuple[str, set[str], set[str]]:
        prefix_wrap: list[str] = []
        suffix_wrap: list[str] = []
        while (enc := _has_enclosing_parens(tokens)) is not None:
            open_i, close_i = enc
            prefix_wrap.append("".join(tk for _, tk in tokens[: open_i + 1]))
            suffix_wrap.append("".join(tk for _, tk in tokens[close_i:]))
            tokens = tokens[open_i + 1 : close_i]

        def _wrap(s: str) -> str:
            if not prefix_wrap:
                return s
            return "".join(prefix_wrap) + s + "".join(reversed(suffix_wrap))

        ctes = dict(inherited_ctes)
        all_canonicals: set[str] = set()
        sig = _sig_indices(tokens)
        if not sig:
            return _wrap("".join(t for _, t in tokens)), set(), set()

        out_prefix = ""
        if tokens[sig[0]][0] == "IDENT" and tokens[sig[0]][1].upper() == "WITH":
            buf: list[str] = []
            w_idx = sig[0]
            buf.extend(tk for _, tk in tokens[: w_idx + 1])
            i_tok = w_idx + 1
            p_rec = i_tok
            while p_rec < len(tokens) and tokens[p_rec][0] in ("WS", "COMMENT"):
                p_rec += 1
            if p_rec < len(tokens) and tokens[p_rec][0] == "IDENT" and tokens[p_rec][1].upper() == "RECURSIVE":
                buf.extend(tk for _, tk in tokens[i_tok : p_rec + 1])
                i_tok = p_rec + 1

            parsed_any_cte = False
            while i_tok < len(tokens):
                p = i_tok
                while p < len(tokens) and tokens[p][0] in ("WS", "COMMENT"):
                    p += 1
                if p >= len(tokens) or tokens[p][0] != "IDENT" or tokens[p][1].upper() == "SELECT":
                    break
                cte_name = tokens[p][1]
                p2 = p + 1
                while p2 < len(tokens) and tokens[p2][0] in ("WS", "COMMENT"):
                    p2 += 1
                if p2 < len(tokens) and tokens[p2] == ("OP", "("):
                    rp_cols = _find_matching_paren(tokens, p2)
                    if not _is_valid_lex_ident_list(tokens[p2 + 1 : rp_cols]):
                        break
                    p2 = rp_cols + 1
                    while p2 < len(tokens) and tokens[p2][0] in ("WS", "COMMENT"):
                        p2 += 1
                if (
                    p2 >= len(tokens)
                    or tokens[p2][0] != "IDENT"
                    or tokens[p2][1].upper() != "AS"
                    or _is_non_kw_lex_pos(tokens, p2)
                ):
                    break
                p2 += 1
                while p2 < len(tokens):
                    if tokens[p2][0] in ("WS", "COMMENT"):
                        p2 += 1
                    elif tokens[p2][0] == "IDENT" and tokens[p2][1].upper() in ("NOT", "MATERIALIZED"):
                        p2 += 1
                    else:
                        break
                if p2 >= len(tokens) or tokens[p2] != ("OP", "("):
                    break
                close_idx = _find_matching_paren(tokens, p2)
                if not _is_subquery_tokens(tokens[p2 + 1 : close_idx]):
                    break
                inner_sql, inner_domains, inner_canonicals = _rewrite_tokens(tokens[p2 + 1 : close_idx], ctes)
                ctes[cte_name.lower()] = inner_domains
                all_canonicals |= inner_canonicals
                buf.extend(tk for _, tk in tokens[i_tok : p2 + 1])
                buf.append(inner_sql)
                buf.append(")")
                i_tok = close_idx + 1
                parsed_any_cte = True

                next_sig = i_tok
                while next_sig < len(tokens) and tokens[next_sig][0] in ("WS", "COMMENT"):
                    next_sig += 1
                if next_sig < len(tokens) and tokens[next_sig] == ("OP", ","):
                    buf.extend(tk for _, tk in tokens[i_tok : next_sig + 1])
                    i_tok = next_sig + 1
                    continue
                break

            if parsed_any_cte:
                out_prefix = "".join(buf)
                tokens = tokens[i_tok:]
                if not tokens:
                    return _wrap(out_prefix), set(), all_canonicals

        top_sig = _sig_indices(tokens)
        top_sig_pos = {idx: pos for pos, idx in enumerate(top_sig)}
        branches: list[tuple[list[tuple[str, str]], list[tuple[str, str]]]] = []
        cur_branch: list[tuple[str, str]] = []
        i_tok = 0
        depth = 0
        while i_tok < len(tokens):
            k, t = tokens[i_tok]
            if k == "OP" and t in ("(", "[", "{"):
                depth += 1
            elif k == "OP" and t in (")", "]", "}"):
                depth = max(0, depth - 1)
            elif (
                depth == 0
                and k == "IDENT"
                and t.upper() in ("UNION", "INTERSECT", "EXCEPT")
                and not _is_non_kw_lex_pos(tokens, i_tok)
                and not _is_lex_dot_qualifier(tokens, top_sig_pos[i_tok], top_sig)
                and not _is_lex_on_expr_ident(tokens, top_sig_pos[i_tok], top_sig)
            ):
                sep: list[tuple[str, str]] = [tokens[i_tok]]
                j = i_tok + 1
                while j < len(tokens) and tokens[j][0] in ("WS", "COMMENT"):
                    sep.append(tokens[j])
                    j += 1
                if j < len(tokens) and tokens[j][0] == "IDENT" and tokens[j][1].upper() in ("ALL", "DISTINCT"):
                    sep.append(tokens[j])
                    j += 1
                branches.append((cur_branch, sep))
                cur_branch = []
                i_tok = j
                continue
            cur_branch.append(tokens[i_tok])
            i_tok += 1
        branches.append((cur_branch, []))

        all_domains: set[str] = set()
        rendered_parts: list[str] = [out_prefix]
        for b_tokens, sep_tokens in branches:
            if _has_enclosing_parens(b_tokens) is not None:
                b_sql, b_domains, b_canonicals = _rewrite_tokens(b_tokens, ctes)
            else:
                b_sql, b_domains, b_canonicals = _rewrite_single_branch(b_tokens, ctes)
            all_domains |= b_domains
            all_canonicals |= b_canonicals
            rendered_parts.append(b_sql)
            rendered_parts.extend(tk for _, tk in sep_tokens)
        return _wrap("".join(rendered_parts)), all_domains, all_canonicals

    def _rewrite_single_branch(
        tokens: list[tuple[str, str]], ctes: dict[str, set[str]]
    ) -> tuple[str, set[str], set[str]]:
        domains: set[str] = set()
        branch_canonicals: set[str] = set()
        alias_domains: dict[str, set[str]] = {}
        processed: list[tuple[str, str]] = []
        subquery_placeholders: dict[str, str] = {}
        subquery_domains: dict[str, set[str]] = {}
        sub_nonce = _secrets.token_hex(6)

        i = 0
        while i < len(tokens):
            k, t = tokens[i]
            if k == "OP" and t == "(":
                close_idx = _find_matching_paren(tokens, i)
                inner = tokens[i + 1 : close_idx]
                prev_sig_idx = next((p for p in range(i - 1, -1, -1) if tokens[p][0] not in ("WS", "COMMENT")), None)
                prev_fn_upper = (
                    tokens[prev_sig_idx][1].upper()
                    if prev_sig_idx is not None and tokens[prev_sig_idx][0] == "IDENT"
                    else ""
                )
                inner_sig = _sig_indices(inner)
                is_trim_from = (
                    prev_fn_upper in ("TRIM", "BTRIM", "LTRIM", "RTRIM")
                    and bool(inner_sig)
                    and inner[inner_sig[0]][0] == "IDENT"
                    and inner[inner_sig[0]][1].upper() == "FROM"
                )
                if not is_trim_from and _is_subquery_tokens(inner):
                    sub_sql, sub_doms, sub_canons = _rewrite_tokens(inner, ctes)
                    branch_canonicals |= sub_canons
                    ph = f"__fla_subq_{sub_nonce}_{len(subquery_placeholders)}__"
                    subquery_placeholders[ph] = f"({sub_sql})"
                    subquery_domains[ph] = sub_doms
                    processed.append(("SUBQ", ph))
                    i = close_idx + 1
                    continue
            processed.append((k, t))
            i += 1

        sig_idxs = _sig_indices(processed)

        def _is_bare_clause_alias_at(cand_s_pos: int) -> bool:
            if cand_s_pos >= len(sig_idxs):
                return False
            ck, ct = processed[sig_idxs[cand_s_pos]]
            if ck != "IDENT":
                return False
            cu = ct.upper()
            after = processed[sig_idxs[cand_s_pos + 1]] if cand_s_pos + 1 < len(sig_idxs) else None
            if cu == "SELECT":
                if after is None or after == ("OP", ","):
                    return True
                if (
                    after[0] == "IDENT"
                    and not _is_lex_dot_qualifier(processed, cand_s_pos + 1, sig_idxs)
                    and after[1].upper() in (_SQL_CLAUSE_STOPWORDS - {"ALL"})
                ):
                    if (
                        after[1].upper() in _FROM_FIRST_SELECT_FN_KEYWORDS
                        and cand_s_pos + 2 < len(sig_idxs)
                        and (
                            processed[sig_idxs[cand_s_pos + 2]] == ("OP", "(")
                            or processed[sig_idxs[cand_s_pos + 2]][0] == "SUBQ"
                        )
                    ):
                        return False
                    return True
                return False
            if cu not in ("PREWHERE", "SAMPLE", "SETTINGS", "FORMAT", "FOR", "SET"):
                return False
            return (
                after is None
                or after in (("OP", ","), ("OP", "("))
                or (after[0] == "IDENT" and after[1].upper() in _SQL_CLAUSE_STOPWORDS)
            )

        table_token_idxs: dict[int, str] = {}
        table_alias_idxs: set[int] = set()
        func_stack: list[str] = []
        in_from = False
        expect_table = False
        in_sample_clause = False
        in_on_clause = False
        depth = 0
        s_pos = 0
        while s_pos < len(sig_idxs):
            idx = sig_idxs[s_pos]
            k, t = processed[idx]
            if k == "OP" and t in ("(", "[", "{"):
                if depth == 0 and in_from and expect_table:
                    raise PermissionError(
                        "parenthesized non-subquery table expressions are not allowed in console queries"
                    )
                if t == "(":
                    prev_fn = processed[sig_idxs[s_pos - 1]] if s_pos > 0 else None
                    func_stack.append(prev_fn[1].upper() if prev_fn and prev_fn[0] == "IDENT" else "")
                else:
                    func_stack.append(t)
                depth += 1
                s_pos += 1
                continue
            if k == "OP" and t in (")", "]", "}"):
                if func_stack:
                    func_stack.pop()
                depth = max(0, depth - 1)
                s_pos += 1
                continue

            if k == "IDENT" and t.upper() == "IN":
                if not (func_stack and func_stack[-1] in ("POSITION", "OVERLAY", "TRIM")):
                    nxt_tok = processed[sig_idxs[s_pos + 1]] if s_pos + 1 < len(sig_idxs) else None
                    if nxt_tok is None or (nxt_tok[0] != "SUBQ" and nxt_tok != ("OP", "(")):
                        raise PermissionError(
                            "IN must be followed by a parenthesized list or subquery in console queries"
                        )

            if depth > 0:
                s_pos += 1
                continue

            if k == "IDENT" and not _is_non_kw_lex_pos(processed, idx):
                wu = t.upper()
                if (
                    _is_lex_dot_qualifier(processed, s_pos, sig_idxs)
                    or (in_on_clause and not expect_table and _is_lex_on_expr_ident(processed, s_pos, sig_idxs))
                    or _is_lex_within_group(processed, s_pos, sig_idxs)
                ):
                    in_sample_clause = False
                    s_pos += 1
                    continue
                if (wu == "FROM" and not _is_lex_distinct_from(processed, s_pos, sig_idxs)) or wu == "JOIN":
                    in_from = True
                    expect_table = True
                    in_sample_clause = False
                    in_on_clause = False
                    s_pos += 1
                    continue
                if wu in _JOIN_MODIFIER_KEYWORDS:
                    if in_from:
                        expect_table = False
                        in_sample_clause = False
                        in_on_clause = False
                    s_pos += 1
                    continue
                if in_from and not expect_table and not in_on_clause and _is_bare_clause_alias_at(s_pos):
                    table_alias_idxs.add(idx)
                    in_sample_clause = False
                    s_pos += 1
                    continue
                if in_from and in_sample_clause and wu == "OFFSET":
                    in_sample_clause = False
                    s_pos += 1
                    continue
                if (wu in _FROM_EXIT_KEYWORDS or (in_from and wu == "SELECT")) and _can_exit_from_lex(
                    processed, s_pos, sig_idxs, in_on_clause=in_on_clause
                ):
                    in_from = False
                    expect_table = False
                    in_sample_clause = False
                    in_on_clause = False
                    s_pos += 1
                    continue
                if in_from and wu in ("ON", "USING", "FINAL", "SAMPLE"):
                    expect_table = False
                    in_sample_clause = wu == "SAMPLE"
                    in_on_clause = wu == "ON"
                    s_pos += 1
                    continue

            if in_from and k == "OP" and t == ",":
                expect_table = True
                in_sample_clause = False
                in_on_clause = False
                s_pos += 1
                continue

            if in_from and expect_table:
                if k == "IDENT" and t.upper() in ("LATERAL", "ONLY"):
                    s_pos += 1
                    continue
                if k == "SUBQ":
                    sub_doms = subquery_domains.get(t, set())
                    domains |= sub_doms
                    a_pos = s_pos + 1
                    if (
                        a_pos < len(sig_idxs)
                        and processed[sig_idxs[a_pos]][0] == "IDENT"
                        and processed[sig_idxs[a_pos]][1].upper() == "AS"
                    ):
                        a_pos += 1
                        if a_pos < len(sig_idxs) and processed[sig_idxs[a_pos]][0] == "IDENT":
                            a_idx = sig_idxs[a_pos]
                            table_alias_idxs.add(a_idx)
                            alias_domains[processed[a_idx][1].lower()] = sub_doms
                            expect_table = False
                            s_pos = a_pos + 1
                            continue
                    elif (
                        a_pos < len(sig_idxs)
                        and processed[sig_idxs[a_pos]][0] == "IDENT"
                        and (
                            processed[sig_idxs[a_pos]][1].upper() not in _SQL_CLAUSE_STOPWORDS
                            or _is_bare_clause_alias_at(a_pos)
                        )
                    ):
                        a_idx = sig_idxs[a_pos]
                        table_alias_idxs.add(a_idx)
                        alias_domains[processed[a_idx][1].lower()] = sub_doms
                        expect_table = False
                        s_pos = a_pos + 1
                        continue
                    expect_table = False
                    s_pos += 1
                    continue
                if k == "IDENT":
                    if s_pos + 1 < len(sig_idxs) and processed[sig_idxs[s_pos + 1]] == ("OP", "("):
                        expect_table = False
                        s_pos += 1
                        continue
                    low_tbl = t.lower()
                    item_doms: set[str] = set()
                    if low_tbl in ctes:
                        item_doms = ctes[low_tbl]
                        domains |= item_doms
                        alias_domains[low_tbl] = item_doms
                    elif low_tbl in table_map:
                        mapped_name, dom, canonical_name = table_map[low_tbl]
                        table_token_idxs[idx] = mapped_name
                        item_doms = {dom}
                        domains.add(dom)
                        branch_canonicals.add(canonical_name)
                        alias_domains[low_tbl] = item_doms
                        alias_domains[mapped_name.lower()] = item_doms
                    a_pos = s_pos + 1
                    if (
                        a_pos < len(sig_idxs)
                        and processed[sig_idxs[a_pos]][0] == "IDENT"
                        and processed[sig_idxs[a_pos]][1].upper() == "AS"
                    ):
                        a_pos += 1
                        if a_pos < len(sig_idxs) and processed[sig_idxs[a_pos]][0] == "IDENT":
                            a_idx = sig_idxs[a_pos]
                            table_alias_idxs.add(a_idx)
                            alias_domains[processed[a_idx][1].lower()] = item_doms
                            expect_table = False
                            s_pos = a_pos + 1
                            continue
                    elif (
                        a_pos < len(sig_idxs)
                        and processed[sig_idxs[a_pos]][0] == "IDENT"
                        and (
                            processed[sig_idxs[a_pos]][1].upper() not in _SQL_CLAUSE_STOPWORDS
                            or _is_bare_clause_alias_at(a_pos)
                        )
                    ):
                        a_idx = sig_idxs[a_pos]
                        table_alias_idxs.add(a_idx)
                        alias_domains[processed[a_idx][1].lower()] = item_doms
                        expect_table = False
                        s_pos = a_pos + 1
                        continue
                    expect_table = False
                    s_pos += 1
                    continue
                expect_table = False

            s_pos += 1

        touches_request = "request" in domains
        touches_any_fact = bool(domains)
        rewritten_tokens: list[tuple[str, str]] = []
        sig_pos_by_idx = {idx: pos for pos, idx in enumerate(sig_idxs)}
        for idx, (k, t) in enumerate(processed):
            if idx in table_token_idxs:
                rewritten_tokens.append(("IDENT", table_token_idxs[idx]))
                continue
            if idx in table_alias_idxs or k != "IDENT":
                rewritten_tokens.append((k, t))
                continue
            s_pos = sig_pos_by_idx[idx]
            prev_tok = processed[sig_idxs[s_pos - 1]] if s_pos > 0 else None
            next_tok = processed[sig_idxs[s_pos + 1]] if s_pos + 1 < len(sig_idxs) else None
            if (
                prev_tok is not None
                and prev_tok[0] == "IDENT"
                and prev_tok[1].upper() == "AS"
                and not _is_non_kw_lex_pos(processed, sig_idxs[s_pos - 1])
            ):
                rewritten_tokens.append((k, t))
                continue
            if next_tok == ("OP", "("):
                rewritten_tokens.append((k, t))
                continue
            if next_tok == ("OP", "."):
                low_q = t.lower()
                if low_q in table_map and low_q not in ctes:
                    rewritten_tokens.append(("IDENT", table_map[low_q][0]))
                else:
                    rewritten_tokens.append((k, t))
                continue
            if prev_tok == ("OP", "."):
                qual_tok = processed[sig_idxs[s_pos - 2]] if s_pos >= 2 else None
                q_domains = (
                    alias_domains.get(qual_tok[1].lower(), domains)
                    if (qual_tok and qual_tok[0] == "IDENT")
                    else domains
                )
                low_c = t.lower()
                if low_c == "timestamp" and q_domains:
                    rewritten_tokens.append(("IDENT", "event_timestamp"))
                elif low_c == "ip" and "request" in q_domains:
                    rewritten_tokens.append(("IDENT", "client_ip"))
                elif low_c in _REQUEST_CF_FIELDS and "request" in q_domains:
                    rewritten_tokens.append(("IDENT", f"custom_fields['{low_c}']"))
                else:
                    rewritten_tokens.append((k, t))
                continue
            low_ident = t.lower()
            if low_ident == "timestamp" and touches_any_fact:
                rewritten_tokens.append(("IDENT", "event_timestamp"))
            elif low_ident == "ip" and touches_request:
                rewritten_tokens.append(("IDENT", "client_ip"))
            elif low_ident in _REQUEST_CF_FIELDS and touches_request:
                rewritten_tokens.append(("IDENT", f"custom_fields['{low_ident}']"))
            else:
                rewritten_tokens.append((k, t))

        if touches_request:
            branch_sql = _coerce_request_predicates(rewritten_tokens)
        else:
            branch_sql = "".join(_normalize_iso(tk) if kd == "STR" else tk for kd, tk in rewritten_tokens)
        for ph, sub_rendered in subquery_placeholders.items():
            branch_sql = branch_sql.replace(ph, sub_rendered)
        return branch_sql, domains, branch_canonicals

    tokens = [(m.lastgroup or "OTHER", m.group(0)) for m in _CONSOLE_SQL_LEX_RE.finditer(sql)]
    rewritten, _, canonicals = _rewrite_tokens(tokens, {})
    return rewritten, canonicals


def _rewrite_console_sql(sql: str, *, svc_table: str, names: Mapping[str, str]) -> str:
    rewritten, _ = _rewrite_console_sql_with_canonicals(sql, svc_table=svc_table, names=names)
    return rewritten


def _scrub_ip_leaves(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _scrub_ip_leaves(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_scrub_ip_leaves(x) for x in obj]
    if isinstance(obj, tuple):
        return tuple(_scrub_ip_leaves(x) for x in obj)
    if isinstance(obj, str) and obj != "[redacted]":
        return scrub_ip_text(obj)
    return obj


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
    names = (
        {canonical: tables[canonical] for canonical in _FACT_DOMAINS if canonical in tables}
        if tables is not None
        else {canonical: canonical for canonical in _FACT_DOMAINS}
    )
    timer = SectionTimer()
    section_timings = timer.entries
    raw_sql = req.sql.strip()
    max_rows = max(1, min(int(req.max_rows), MAX_QUERY_ROWS))
    svc_table = _safe_table(service.service_id)
    allowed_tables = frozenset(
        {
            "logs",
            "client_vitals",
            "client_errors",
            "request_facts",
            "rum_vitals_facts",
            "rum_error_facts",
            "rum_page_load_facts",
            svc_table.lower(),
        }
    )
    _t_val = time.perf_counter()
    _reject_ambiguous_syntax(raw_sql, allowed_tables | frozenset(names.values()))
    try:
        validate_user_sql(
            raw_sql,
            session_id=session_id,
            service_id=service.service_id,
            allowed_tables=allowed_tables,
        )
    except SQLValidationError as exc:
        raise PermissionError(exc.message) from exc
    timer.mark("validate_user_sql", _t_val)
    sql, referenced_canonicals = _rewrite_console_sql_with_canonicals(
        raw_sql.rstrip().rstrip(";"), svc_table=svc_table, names=names
    )

    post_allowed_tables = frozenset(t.lower() for t in names.values())
    _reject_ambiguous_syntax(sql, allowed_tables | post_allowed_tables)
    try:
        post_val = validate_user_sql(
            sql,
            session_id=session_id,
            service_id=service.service_id,
            allowed_tables=post_allowed_tables,
        )
    except SQLValidationError as exc:
        raise PermissionError(exc.message) from exc

    def _collect_non_cte_base_tables(node_obj: Any, active_ctes: frozenset[str] = frozenset()) -> set[str]:
        found: set[str] = set()
        if isinstance(node_obj, dict):
            if isinstance(cte_map := node_obj.get("cte_map"), dict) and isinstance(entries := cte_map.get("map"), list):
                for entry in entries:
                    if isinstance(entry, dict):
                        key = entry.get("key")
                        alias_fs = frozenset({key.strip().lower()}) if isinstance(key, str) and key else frozenset()
                        if (val := entry.get("value")) is not None:
                            found |= _collect_non_cte_base_tables(val, active_ctes)
                        active_ctes = active_ctes | alias_fs
            if node_obj.get("type") == "BASE_TABLE" or "table_name" in node_obj:
                tname = str(node_obj.get("table_name") or "").strip().lower()
                sname = str(node_obj.get("schema_name") or "").strip().lower()
                cname = str(node_obj.get("catalog_name") or "").strip().lower()
                if tname and not (tname in active_ctes and not sname and not cname):
                    found.add(tname)
            for k_child, v_child in node_obj.items():
                if k_child != "cte_map":
                    found |= _collect_non_cte_base_tables(v_child, active_ctes)
        elif isinstance(node_obj, list):
            for item in node_obj:
                found |= _collect_non_cte_base_tables(item, active_ctes)
        return found

    code_sql = _code_only(sql)
    fact_tables = {
        names[canonical]: domain
        for canonical, domain in _FACT_DOMAINS.items()
        if canonical in names
        and (
            canonical in referenced_canonicals
            or re.search(rf"\b{re.escape(names[canonical])}\b", code_sql, flags=re.IGNORECASE)
        )
    }
    non_cte_tables = _collect_non_cte_base_tables(post_val.parse_tree)
    resolved_fact_lower = {tbl.lower() for tbl in fact_tables}
    if non_cte_tables and (not fact_tables or not non_cte_tables.issubset(resolved_fact_lower)):
        raise PermissionError("Query referenced an allowed table without resolving a scoped fact table")
    if any(re.search(rf"\b{re.escape(tbl)}\b", code_sql, flags=re.IGNORECASE) for tbl in post_allowed_tables):
        if not fact_tables:
            raise PermissionError("Query referenced an allowed table without resolving a scoped fact table")
    has_explicit_limit = bool(re.search(r"\bLIMIT\b", code_sql, flags=re.IGNORECASE))
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
    cache_key: tuple | None = None
    if not os.getenv("PYTEST_CURRENT_TEST"):
        cache_key = (
            service.service_id,
            req.dataset,
            sql,
            max_rows,
            bool(req.explain),
            mask_ips,
            start_time,
            end_time,
            tuple(sorted((tables or {}).items())),
            os.getenv("DEBUG_RESPONSES", ""),
        )
        if (cached_res := _HS_QUERY_RESULT_CACHE.get(cache_key)) is not None:
            return {**cached_res, "_section_timings": section_timings}
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
    for d in res:
        row_copy = dict(d)
        for k, v in row_copy.items():
            if hasattr(v, "isoformat"):
                val = v.isoformat()
                row_copy[k] = val.replace("+00:00", "Z") if "+" in val else val + "Z"
            elif mask_ips and is_saved_view_pii_key(str(k)) and v:
                row_copy[k] = "[redacted]"
        data.append(row_copy)
    if mask_ips:
        data = apply_pii_policy(data, {"mask_ips": True})
        data = mask_ip_values(data)
        data = _scrub_ip_leaves(data)
    timer.mark("serialize_json", _t_serialize)
    out = build_query_response_envelope(
        columns=cols,
        data=data,
        total_rows=total_rows,
        truncated=truncated,
        elapsed_ms=elapsed_ms,
        debug_queries=_debug_queries,
        debug_calls=get_tracked_calls(),
        section_timings=section_timings,
    )
    if cache_key is not None:
        _HS_QUERY_RESULT_CACHE[cache_key] = dict(out)
    return out
