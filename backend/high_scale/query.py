"""Lossless source-object decoder and bounded console SQL execution for high-scale."""

from __future__ import annotations

import base64
import gzip
import inspect
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

from backend.core.catalog import _CANONICAL_SYSTEM_FIELD_SPECS
from backend.core.log_fields import LOG_FIELD_CATALOG, SqlType, try_get
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
    elif k.lower().startswith(("cmcd_", "cmcd.", "cmcd-")):
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
                    if short_k.lower() != short_k:
                        cmcd[short_k.lower()] = s_val
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


def _collect_ast_base_tables_for_exempt(node_obj: Any) -> set[str]:
    found: set[str] = set()
    if isinstance(node_obj, dict):
        if node_obj.get("type") == "BASE_TABLE" or "table_name" in node_obj:
            tname = str(node_obj.get("table_name") or "").strip().lower()
            if tname:
                found.add(tname)
        for v_child in node_obj.values():
            found |= _collect_ast_base_tables_for_exempt(v_child)
    elif isinstance(node_obj, list):
        for item in node_obj:
            found |= _collect_ast_base_tables_for_exempt(item)
    return found


def _exempt_col_list_ident_starts(
    sql: str,
    *,
    parse_tree: Any | None = None,
    _ast_fn_budget_out: dict[str, int] | None = None,
) -> set[int]:
    """Return character start offsets of valid CTE header and FROM/JOIN column-list aliases."""
    from backend.core import sql_validator as _sv

    eff_tree = parse_tree
    if eff_tree is None:
        eff_tree, _n, err_c, _err_m = _sv._parse_single_select_node(sql)
        if err_c is not None:
            eff_tree = None
    known_relations: set[str] | None = _collect_ast_base_tables_for_exempt(eff_tree) if eff_tree is not None else None

    sig = [
        (m.lastgroup or "OTHER", m.group(0), m.start())
        for m in _CONSOLE_SQL_LEX_RE.finditer(sql)
        if (m.lastgroup or "OTHER") not in ("WS", "COMMENT")
    ]
    exempt: set[int] = set()
    subquery_preceding_keywords = frozenset(
        {
            "FROM",
            "JOIN",
            "AS",
            "IN",
            "EXISTS",
            "WITH",
            "SELECT",
            "WHERE",
            "ON",
            "AND",
            "OR",
            "XOR",
            "NOT",
            "WHEN",
            "THEN",
            "ELSE",
            "CASE",
            "ALL",
            "ANY",
            "SOME",
            "UNION",
            "EXCEPT",
            "INTERSECT",
            "LATERAL",
            "ONLY",
            "PREWHERE",
            "HAVING",
            "QUALIFY",
            "LIMIT",
            "OFFSET",
            "BY",
            "BETWEEN",
            "IS",
            "DISTINCT",
            "OVER",
            "RETURN",
        }
    )
    non_table_or_alias_keywords = (
        _SQL_CLAUSE_STOPWORDS
        | _ON_EXPR_PREV_WORDS
        | _ON_EXPR_NEXT_WORDS
        | frozenset(w.upper() for w in _SQL_KEYWORDS)
        | frozenset(
            w.upper()
            for w in (_sv._SPECIAL_CALL_KEYWORDS | _sv._NON_FUNCTION_KEYWORDS | _sv._FROM_FIRST_SELECT_FN_KEYWORDS)
        )
        | frozenset(
            {
                "CASE",
                "NOT",
                "EXISTS",
                "SELECT",
                "FROM",
                "WHERE",
                "JOIN",
                "ON",
                "WHEN",
                "THEN",
                "ELSE",
                "END",
                "ARRAY",
                "WITH",
                "AS",
                "FINAL",
                "SAMPLE",
                "GLOBAL",
                "ANY",
                "ALL",
                "ASOF",
                "SEMI",
                "ANTI",
                "LATERAL",
                "VALUES",
                "TABLE",
                "ONLY",
                "TRUE",
                "FALSE",
                "NULL",
                "IS",
                "IN",
                "BETWEEN",
                "LIKE",
                "ILIKE",
                "CAST",
                "TRY_CAST",
                "DISTINCT",
                "OVER",
                "FILTER",
                "WINDOW",
                "QUALIFY",
                "SOME",
                "MINUS",
                "PLUS",
                "BINARY",
                "UNIQUE",
                "PRIOR",
                "BNOT",
                "BITNOT",
                "UNNEST",
                "COLUMNS",
                "STRUCT",
                "MAP",
                "ROW",
                "LIST",
                "TUPLE",
                "DATE",
                "TIME",
                "TIMESTAMP",
                "TIMESTAMPTZ",
                "JSON",
                "UUID",
                "BLOB",
                "OPERATOR",
            }
        )
    )
    valid_post_col_list_keywords = (
        _FROM_EXIT_KEYWORDS
        | _JOIN_MODIFIER_KEYWORDS
        | frozenset({"ON", "USING", "JOIN", "SELECT", "FINAL", "SAMPLE", "ARRAY"})
    )

    def _unquote_tok(kind: str, raw: str) -> str:
        if kind == "DQUOTE" and len(raw) >= 2 and raw[0] == raw[-1]:
            q = raw[0]
            return raw[1:-1].replace(q + q, q)
        return raw

    def _match_delim(s_list: list[tuple[str, str, int]], open_i: int, open_ch: str = "(", close_ch: str = ")") -> int:
        depth = 0
        for idx in range(open_i, len(s_list)):
            k, t, _ = s_list[idx]
            if k == "OP" and t == open_ch:
                depth += 1
            elif k == "OP" and t == close_ch:
                depth -= 1
                if depth == 0:
                    return idx
        return -1

    def _is_fn_call_open(s_list: list[tuple[str, str, int]], open_i: int) -> bool:
        if open_i <= 0:
            return False
        prev_k, prev_t, _ = s_list[open_i - 1]
        if prev_k == "DQUOTE":
            return True
        if prev_k == "IDENT":
            if open_i >= 2 and (s_list[open_i - 2][0], s_list[open_i - 2][1]) == ("OP", "."):
                return True
            return prev_t.upper() not in subquery_preceding_keywords
        return False

    def _valid_ident_list(sub_s: list[tuple[str, str, int]]) -> bool:
        if not sub_s or len(sub_s) % 2 == 0:
            return False
        return all(
            (sub_s[p][0] in ("IDENT", "DQUOTE") if p % 2 == 0 else (sub_s[p][0], sub_s[p][1]) == ("OP", ","))
            for p in range(len(sub_s))
        )

    def _looks_like_subquery(sub_s: list[tuple[str, str, int]]) -> bool:
        cur = sub_s
        while len(cur) >= 2 and (cur[0][0], cur[0][1]) == ("OP", "("):
            close_first = _match_delim(cur, 0)
            if close_first == len(cur) - 1:
                cur = cur[1:-1]
                continue
            if (
                0 < close_first < len(cur) - 1
                and cur[close_first + 1][0] == "IDENT"
                and cur[close_first + 1][1].upper()
                in ("UNION", "INTERSECT", "EXCEPT", "ORDER", "LIMIT", "OFFSET", "FETCH")
            ):
                return _looks_like_subquery(cur[1:close_first])
            break
        return bool(cur) and cur[0][0] == "IDENT" and cur[0][1].upper() in ("SELECT", "WITH", "FROM", "TABLE")

    def _walk_expr_tokens(sub_s: list[tuple[str, str, int]], ctes_in_scope: frozenset[str]) -> None:
        idx = 0
        while idx < len(sub_s):
            k_e, t_e, _ = sub_s[idx]
            if k_e == "OP" and t_e == "(":
                cl = _match_delim(sub_s, idx, "(", ")")
                if cl > idx:
                    if _is_fn_call_open(sub_s, idx):
                        _walk_expr_tokens(sub_s[idx + 1 : cl], ctes_in_scope)
                    else:
                        _walk_paren_group(sub_s[idx + 1 : cl], ctes_in_scope)
                    idx = cl + 1
                    continue
            elif k_e == "OP" and t_e in ("[", "{"):
                cl_b = _match_delim(sub_s, idx, t_e, "]" if t_e == "[" else "}")
                if cl_b > idx:
                    _walk_expr_tokens(sub_s[idx + 1 : cl_b], ctes_in_scope)
                    idx = cl_b + 1
                    continue
            idx += 1

    def _walk_paren_group(sub_s: list[tuple[str, str, int]], ctes_in_scope: frozenset[str]) -> None:
        if _looks_like_subquery(sub_s):
            _walk_query(sub_s, ctes_in_scope)
            return
        _walk_expr_tokens(sub_s, ctes_in_scope)

    def _valid_post_col_list_boundary(s_list: list[tuple[str, str, int]], next_i: int) -> bool:
        if next_i >= len(s_list):
            return True
        nk, nt, _ = s_list[next_i]
        if (nk, nt) in (("OP", ","), ("OP", ";")):
            return True
        return nk == "IDENT" and nt.upper() in valid_post_col_list_keywords

    def _consume_alias_col_list(
        s_list: list[tuple[str, str, int]],
        a_i: int,
        *,
        record_ast_fn: bool = True,
    ) -> int | None:
        n = len(s_list)
        if a_i >= n:
            return None
        if s_list[a_i][0] == "IDENT" and s_list[a_i][1].upper() == "AS":
            a_i += 1
            if a_i >= n or s_list[a_i][0] not in ("IDENT", "DQUOTE"):
                return None
            if s_list[a_i][0] == "IDENT" and s_list[a_i][1].upper() in non_table_or_alias_keywords:
                return None
            alias_tok = s_list[a_i]
            lp_i = a_i + 1
        elif s_list[a_i][0] in ("IDENT", "DQUOTE") and (
            s_list[a_i][0] == "DQUOTE" or s_list[a_i][1].upper() not in non_table_or_alias_keywords
        ):
            alias_tok = s_list[a_i]
            lp_i = a_i + 1
        else:
            return None
        if lp_i < n and (s_list[lp_i][0], s_list[lp_i][1]) == ("OP", "("):
            rp_i = _match_delim(s_list, lp_i)
            if (
                rp_i > lp_i
                and _valid_ident_list(s_list[lp_i + 1 : rp_i])
                and _valid_post_col_list_boundary(s_list, rp_i + 1)
            ):
                if alias_tok[0] == "IDENT":
                    exempt.add(alias_tok[2])
                    if record_ast_fn and _ast_fn_budget_out is not None:
                        a_low = alias_tok[1].lower()
                        _ast_fn_budget_out[a_low] = _ast_fn_budget_out.get(a_low, 0) + 1
                return rp_i + 1
        return None

    def _walk_query(s_list: list[tuple[str, str, int]], inherited_ctes: frozenset[str] = frozenset()) -> None:
        while (
            len(s_list) >= 2
            and (s_list[0][0], s_list[0][1]) == ("OP", "(")
            and _match_delim(s_list, 0) == len(s_list) - 1
        ):
            s_list = s_list[1:-1]
        if not s_list:
            return
        in_scope_ctes: set[str] = set(inherited_ctes)
        pos = 0
        n = len(s_list)
        if s_list[0][0] == "IDENT" and s_list[0][1].upper() == "WITH":
            p = 1
            is_recursive = False
            if p < n and s_list[p][0] == "IDENT" and s_list[p][1].upper() == "RECURSIVE":
                is_recursive = True
                p += 1
            parsed_cte = False
            while p < n:
                if s_list[p][0] not in ("IDENT", "DQUOTE") or (
                    s_list[p][0] == "IDENT" and s_list[p][1].upper() in non_table_or_alias_keywords
                ):
                    break
                cte_tok = s_list[p]
                cte_low = _unquote_tok(cte_tok[0], cte_tok[1]).lower()
                p2 = p + 1
                has_col_list = False
                if p2 < n and (s_list[p2][0], s_list[p2][1]) == ("OP", "("):
                    rp = _match_delim(s_list, p2)
                    if rp <= p2 or not _valid_ident_list(s_list[p2 + 1 : rp]):
                        break
                    has_col_list = True
                    p2 = rp + 1
                if p2 >= n or s_list[p2][0] != "IDENT" or s_list[p2][1].upper() != "AS":
                    break
                p2 += 1
                while p2 < n and s_list[p2][0] == "IDENT" and s_list[p2][1].upper() in ("NOT", "MATERIALIZED"):
                    p2 += 1
                if p2 >= n or (s_list[p2][0], s_list[p2][1]) != ("OP", "("):
                    break
                close_cte = _match_delim(s_list, p2)
                if close_cte <= p2 or not _looks_like_subquery(s_list[p2 + 1 : close_cte]):
                    break
                if has_col_list and cte_tok[0] == "IDENT":
                    exempt.add(cte_tok[2])
                body_ctes = frozenset(in_scope_ctes | ({cte_low} if is_recursive else set()))
                _walk_query(s_list[p2 + 1 : close_cte], body_ctes)
                in_scope_ctes.add(cte_low)
                p = close_cte + 1
                parsed_cte = True
                if p < n and (s_list[p][0], s_list[p][1]) == ("OP", ","):
                    p += 1
                    continue
                break
            if parsed_cte:
                pos = p

        active_ctes = frozenset(in_scope_ctes)
        in_from = False
        in_array_join = False
        expect_table = False
        in_on = False
        while pos < n:
            k, t, _ = s_list[pos]
            if (k, t) == ("OP", "("):
                cl = _match_delim(s_list, pos)
                if cl <= pos:
                    break
                inner = s_list[pos + 1 : cl]
                is_fn_call = _is_fn_call_open(s_list, pos)
                if not is_fn_call and in_from and expect_table and _looks_like_subquery(inner):
                    _walk_query(inner, active_ctes)
                    end_alias = _consume_alias_col_list(s_list, cl + 1, record_ast_fn=True)
                    expect_table = False
                    pos = end_alias if end_alias is not None else cl + 1
                    continue
                if is_fn_call:
                    _walk_expr_tokens(inner, active_ctes)
                else:
                    _walk_paren_group(inner, active_ctes)
                if in_from and expect_table:
                    expect_table = False
                pos = cl + 1
                continue
            if k == "OP" and t in ("[", "{"):
                cl_br = _match_delim(s_list, pos, t, "]" if t == "[" else "}")
                if cl_br <= pos:
                    break
                _walk_expr_tokens(s_list[pos + 1 : cl_br], active_ctes)
                if in_from and expect_table:
                    expect_table = False
                pos = cl_br + 1
                continue
            prev_is_dot = pos >= 1 and (s_list[pos - 1][0], s_list[pos - 1][1]) == ("OP", ".")
            prev_is_as_kw = (
                not expect_table
                and pos >= 1
                and s_list[pos - 1][0] == "IDENT"
                and s_list[pos - 1][1].upper() == "AS"
                and not (pos >= 2 and (s_list[pos - 2][0], s_list[pos - 2][1]) == ("OP", "."))
            )
            prev_is_expr_op = (
                pos >= 1
                and s_list[pos - 1][0] == "OP"
                and s_list[pos - 1][1] in ("=", "!=", "<>", "<=", ">=", "<", ">", "+", "-", "/", "[")
            )
            if k == "IDENT" and not prev_is_dot and not prev_is_as_kw and not prev_is_expr_op:
                wu = t.upper()
                is_distinct_from = (
                    wu == "FROM"
                    and pos >= 2
                    and s_list[pos - 1][0] == "IDENT"
                    and s_list[pos - 1][1].upper() == "DISTINCT"
                )
                is_prev_array = (
                    pos >= 1
                    and s_list[pos - 1][0] == "IDENT"
                    and s_list[pos - 1][1].upper() == "ARRAY"
                    and not (pos >= 2 and (s_list[pos - 2][0], s_list[pos - 2][1]) == ("OP", "."))
                )
                if in_from and not expect_table and wu == "ARRAY":
                    in_array_join = True
                    expect_table = False
                    in_on = False
                    pos += 1
                    continue
                if in_from and wu == "JOIN" and is_prev_array:
                    in_array_join = True
                    expect_table = False
                    in_on = False
                    pos += 1
                    continue
                if (wu == "FROM" and not is_distinct_from) or (in_from and not expect_table and wu == "JOIN"):
                    in_from = True
                    in_array_join = False
                    expect_table = True
                    in_on = False
                    pos += 1
                    continue
                if in_from and not expect_table and wu in _JOIN_MODIFIER_KEYWORDS:
                    expect_table = False
                    pos += 1
                    continue
                if in_from and expect_table and wu in ("LATERAL", "ONLY"):
                    pos += 1
                    continue
                if in_from and not expect_table and (wu == "ON" or (wu == "USING" and not in_on)):
                    expect_table = False
                    in_on = wu == "ON"
                    pos += 1
                    continue
                if wu == "SELECT" and in_from and not expect_table and not in_on:
                    nxt_t = s_list[pos + 1] if pos + 1 < n else None
                    is_fn_proj = (
                        nxt_t is not None
                        and nxt_t[0] == "IDENT"
                        and nxt_t[1].upper() in _sv._FROM_FIRST_SELECT_FN_KEYWORDS
                        and pos + 2 < n
                        and (s_list[pos + 2][0], s_list[pos + 2][1]) == ("OP", "(")
                    )
                    nxt_is_dot = pos + 2 < n and (s_list[pos + 2][0], s_list[pos + 2][1]) == ("OP", ".")
                    is_bare_sel_alias = not is_fn_proj and (
                        nxt_t is None
                        or (nxt_t[0], nxt_t[1]) == ("OP", ",")
                        or (
                            nxt_t[0] == "IDENT"
                            and not nxt_is_dot
                            and nxt_t[1].upper() in (_SQL_CLAUSE_STOPWORDS - {"ALL"})
                        )
                    )
                    if is_bare_sel_alias:
                        pos += 1
                        continue
                is_within_group = (
                    wu == "GROUP"
                    and pos >= 1
                    and s_list[pos - 1][0] == "IDENT"
                    and s_list[pos - 1][1].upper() == "WITHIN"
                )
                can_exit_on = True
                if in_on and wu in ("GROUP", "ORDER"):
                    can_exit_on = (
                        pos + 1 < n and s_list[pos + 1][0] == "IDENT" and s_list[pos + 1][1].upper() in ("BY", "ALL")
                    )
                elif in_on and wu == "FETCH":
                    can_exit_on = (
                        pos + 1 < n
                        and s_list[pos + 1][0] == "IDENT"
                        and s_list[pos + 1][1].upper() in ("FIRST", "NEXT")
                    )
                if (
                    not is_within_group
                    and can_exit_on
                    and (wu in _FROM_EXIT_KEYWORDS or (in_from and not expect_table and wu == "SELECT"))
                ):
                    in_from = False
                    in_array_join = False
                    expect_table = False
                    in_on = False
                    pos += 1
                    continue
            if in_from and not expect_table and (k, t) == ("OP", ","):
                in_on = False
                expect_table = not in_array_join
                pos += 1
                continue
            if in_from and expect_table:
                if k in ("IDENT", "DQUOTE"):
                    parts = [s_list[pos]]
                    j = pos + 1
                    while (
                        j + 1 < n
                        and (s_list[j][0], s_list[j][1]) == ("OP", ".")
                        and s_list[j + 1][0] in ("IDENT", "DQUOTE")
                    ):
                        parts.append(s_list[j + 1])
                        j += 2
                    if j < n and (s_list[j][0], s_list[j][1]) == ("OP", "("):
                        cl_fn = _match_delim(s_list, j)
                        if cl_fn > j:
                            _walk_expr_tokens(s_list[j + 1 : cl_fn], active_ctes)
                            fn_low = parts[0][1].lower()
                            if (
                                len(parts) == 1
                                and parts[0][0] == "IDENT"
                                and fn_low in _sv._ALLOWED_TABLE_FUNCTIONS
                                and fn_low in _ALLOWED_FUNCTIONS
                            ):
                                end_tf_alias = _consume_alias_col_list(s_list, cl_fn + 1, record_ast_fn=False)
                                pos = end_tf_alias if end_tf_alias is not None else cl_fn + 1
                            else:
                                pos = cl_fn + 1
                        else:
                            pos = j + 1
                        expect_table = False
                        continue
                    if (
                        len(parts) == 1
                        and parts[0][0] == "IDENT"
                        and parts[0][1].upper() in non_table_or_alias_keywords
                    ):
                        expect_table = False
                        pos = j
                        continue
                    tbl_low = _unquote_tok(parts[-1][0], parts[-1][1]).lower()
                    if known_relations is not None and tbl_low not in (known_relations | active_ctes):
                        expect_table = False
                        pos = j
                        continue
                    end_alias = _consume_alias_col_list(s_list, j, record_ast_fn=True)
                    expect_table = False
                    pos = end_alias if end_alias is not None else j
                    continue
                expect_table = False
                pos += 1
                continue
            pos += 1

    _walk_query(sig)
    return exempt


def _reject_unsafe_functions(sql: str, *, parse_tree: Any | None = None) -> None:
    from backend.core import sql_validator as _sv

    eff_tree = parse_tree
    if eff_tree is None:
        eff_tree, _n, err_c, _err_m = _sv._parse_single_select_node(sql)
        if err_c is not None:
            eff_tree = None

    sig = [
        (m.lastgroup or "OTHER", m.group(0), m.start())
        for m in _CONSOLE_SQL_LEX_RE.finditer(sql)
        if (m.lastgroup or "OTHER") not in ("WS", "COMMENT")
    ]
    ast_fn_budget: dict[str, int] = {}
    exempt_starts = _exempt_col_list_ident_starts(sql, parse_tree=eff_tree, _ast_fn_budget_out=ast_fn_budget)
    for idx in range(len(sig) - 1):
        if (sig[idx + 1][0], sig[idx + 1][1]) != ("OP", "("):
            continue
        kind, tok, start_off = sig[idx]
        if kind == "DQUOTE":
            raise PermissionError("quoted function names are not allowed")
        if kind != "IDENT":
            continue
        if start_off in exempt_starts:
            continue
        lowered = tok.lower()
        if lowered in _SQL_KEYWORDS or lowered in _ALLOWED_FUNCTIONS:
            continue
        raise PermissionError(f"function {tok!r} is not allowed in console queries")

    if eff_tree is not None:

        def _check_ast_fn_nodes(node_obj: Any, *, preceded_by_rel: bool = False) -> None:
            if isinstance(node_obj, dict):
                fname = node_obj.get("function_name")
                if isinstance(fname, str) and fname:
                    fl = fname.lower()
                    if node_obj.get("type") == "TABLE_FUNCTION":
                        if fl not in _ALLOWED_FUNCTIONS:
                            raise PermissionError(f"function {fname!r} is not allowed in console queries")
                    elif fl not in _SQL_KEYWORDS and fl not in _ALLOWED_FUNCTIONS:
                        rem = ast_fn_budget.get(fl, 0)
                        if preceded_by_rel and rem > 0:
                            ast_fn_budget[fl] = rem - 1
                        else:
                            raise PermissionError(f"function {fname!r} is not allowed in console queries")
                for key_k, child_v in node_obj.items():
                    if key_k == "children" and isinstance(child_v, list):
                        prev_is_rel = False
                        for item in child_v:
                            _check_ast_fn_nodes(item, preceded_by_rel=prev_is_rel)
                            prev_is_rel = isinstance(item, dict) and item.get("type") in (
                                "BASE_TABLE",
                                "SELECT_NODE",
                                "SET_OPERATION_NODE",
                                "RECURSIVE_CTE_NODE",
                            )
                    else:
                        _check_ast_fn_nodes(child_v, preceded_by_rel=False)
            elif isinstance(node_obj, list):
                for item in node_obj:
                    _check_ast_fn_nodes(item, preceded_by_rel=False)

        _check_ast_fn_nodes(eff_tree)


_HS_QUERY_RESULT_CACHE: BoundedTTLCache = BoundedTTLCache(maxsize=256, ttl_seconds=15.0)
CacheRegistry.register("high_scale.query._HS_QUERY_RESULT_CACHE", _HS_QUERY_RESULT_CACHE)

_CONSOLE_SQL_LEX_RE = re.compile(
    r"(?P<COMMENT>--[^\n]*|/\*[\s\S]*?\*/)"
    r"|(?P<STR>'(?:''|[^'])*')"
    r'|(?P<DQUOTE>"(?:""|[^"])*"|`(?:``|[^`])*`)'
    r"|(?P<WS>\s+)"
    r'|(?P<NUM>(?:0(?:[xX][0-9a-fA-F][0-9a-fA-F_]*(?:\.(?!\.)[0-9a-fA-F_]*)?(?:[pP][+-]?\d+(?:_\d+)*)?|[bB][01][01_]*|[oO][0-7][0-7_]*)|(?:\d+(?:_\d+)*(?:\.(?!\.)(?:\d+(?:_\d+)*)?)?|(?<![A-Za-z0-9_)\]}"\'`])\.\d+(?:_\d+)*)(?:[eE][+-]?\d+(?:_\d+)*)?))'
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
        "NOT",
        "AT",
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
_AS_CLAUSE_OPENING_WORDS = frozenset(
    {
        "SELECT",
        "WITH",
        "WHERE",
        "PREWHERE",
        "HAVING",
        "QUALIFY",
        "LIMIT",
        "OFFSET",
        "BY",
        "FROM",
        "JOIN",
        "TABLE",
        "USING",
    }
)
_SQL_INTERVAL_UNITS = frozenset(
    {
        "YEAR",
        "YEARS",
        "QUARTER",
        "QUARTERS",
        "MONTH",
        "MONTHS",
        "WEEK",
        "WEEKS",
        "DAY",
        "DAYS",
        "HOUR",
        "HOURS",
        "MINUTE",
        "MINUTES",
        "SECOND",
        "SECONDS",
        "MILLISECOND",
        "MILLISECONDS",
        "MICROSECOND",
        "MICROSECONDS",
        "NANOSECOND",
        "NANOSECONDS",
    }
)
_ON_OPERATOR_CONTEXT_WORDS = frozenset(
    {
        "SELECT",
        "DISTINCT",
        "ALL",
        "WHERE",
        "PREWHERE",
        "HAVING",
        "QUALIFY",
        "ON",
        "AND",
        "OR",
        "XOR",
        "NOT",
        "IS",
        "IN",
        "LIKE",
        "ILIKE",
        "RLIKE",
        "REGEXP",
        "GLOB",
        "SIMILAR",
        "TO",
        "BETWEEN",
        "CASE",
        "WHEN",
        "THEN",
        "ELSE",
        "DIV",
        "MOD",
        "COLLATE",
    }
)
_FROM_FIRST_SELECT_FN_KEYWORDS = frozenset({"ANY", "ARRAY", "LEFT", "RIGHT", "FORMAT", "SAMPLE", "WINDOW"})
_REQUEST_TYPED_COLUMNS = frozenset(
    {
        "service_id",
        "event_id",
        "event_timestamp",
        "ingest_timestamp",
        "source_object_key",
        "source_object_version",
        "line_ordinal",
        "transform_version",
        "batch_id",
        "publication_state",
        "country",
        "client_ip",
        "url",
        "custom_fields",
        "cmcd",
    }
)
_EXTRA_DUCKDB_REQUEST_FIELDS = frozenset(
    {
        "_bot_name",
        "as_name",
        "asset_type",
        "bot_category",
        "bot_score",
        "bot_verified",
        "bytes",
        "cache_control",
        "cache_status",
        "cid",
        "client_asn",
        "client_cwnd",
        "client_ploss",
        "client_rtt_us",
        "client_tcpi_delta_retrans",
        "client_tcpi_rcv_space",
        "client_tcpi_rtt",
        "client_tcpi_rttvar",
        "client_tcpi_snd_mss",
        "cmcd_nor",
        "cmcd_nrr",
        "cmcd_pr",
        "cmcd_v",
        "content_encoding",
        "content_type",
        "cwnd",
        "ddos_action",
        "fastly_is_shield",
        "fastly_pop",
        "geo_city",
        "geo_country",
        "geo_region",
        "h2_fingerprint",
        "h2fp",
        "ifmt",
        "io_transform",
        "ipv6",
        "is_bot",
        "is_edge",
        "is_shield",
        "is_ssl",
        "ja3_md5",
        "ngwaf_bot",
        "ngwaf_bot_category",
        "ngwaf_bot_name",
        "ngwaf_decision",
        "ngwaf_signals",
        "ngwaf_wellknown_bot",
        "ofmt",
        "oh_fingerprint",
        "ohbytes",
        "ohfp",
        "origin_host",
        "origin_ip",
        "origin_status",
        "origin_time_ms",
        "origin_ttfb_ms",
        "path",
        "protocol",
        "query_string",
        "req_body_bytes",
        "req_body_size",
        "req_id",
        "resp_body_bytes",
        "resp_body_size",
        "resp_header_bytes",
        "resp_header_content_type",
        "surrogate_control",
        "tcpi_delivery_rate",
        "tcpi_min_rtt",
        "tcpi_rtt",
        "tcpi_rtt_var",
        "tcpi_snd_cwnd",
        "tcpi_total_retrans",
        "time_elapsed_usec",
        "time_to_first_byte_ns",
        "tls_cipher",
        "tls_version",
        "ttfb_ms",
        "user_agent",
        "verified_bot",
        "waf_action",
        "waf_blocked_rules",
        "waf_logged_rules",
        "waf_message",
        "waf_rule_id",
        "waf_severity",
    }
)
_REQUEST_CF_FIELDS = (
    (
        frozenset(_FIELD_DIMENSIONS.values())
        | frozenset(
            str(f["id"]).lower() for f in LOG_FIELD_CATALOG if f.get("group") not in ("METRICS", "VIRTUAL", "INTERNAL")
        )
        | frozenset(k.lower() for k, (stg, _) in _CANONICAL_SYSTEM_FIELD_SPECS.items() if stg != "beacon")
        | _EXTRA_DUCKDB_REQUEST_FIELDS
    )
    - (_REQUEST_TYPED_COLUMNS | {"timestamp", "ip"})
    - _SQL_KEYWORDS
    - frozenset(
        w.lower() for w in (_SQL_CLAUSE_STOPWORDS | _ON_EXPR_PREV_WORDS | _ON_EXPR_NEXT_WORDS | _SQL_INTERVAL_UNITS)
    )
    - {"asc", "desc", "null", "true", "false", "nulls", "first", "last"}
)
_REQUEST_BOOLEAN_CF_FIELDS = frozenset(
    {
        "edge",
        "is_ipv6",
        "waf",
        "is_bot",
        "verified_bot",
        "bot_verified",
        "is_ssl",
        "ipv6",
        "is_shield",
        "fastly_is_shield",
        "is_edge",
        "cmcd_bs",
        "cmcd_su",
    }
) | frozenset(str(f["id"]).lower() for f in LOG_FIELD_CATALOG if str(f.get("sql_type") or "").upper() == "BOOLEAN")


def _unquote_sql_ident(token_text: str) -> str:
    if len(token_text) >= 2 and token_text[0] == '"' and token_text[-1] == '"':
        return token_text[1:-1].replace('""', '"')
    if len(token_text) >= 2 and token_text[0] == "`" and token_text[-1] == "`":
        return token_text[1:-1].replace("``", "`")
    return token_text


def _reject_internal_cf_identifiers(sql: str) -> None:
    for m in _CONSOLE_SQL_LEX_RE.finditer(sql):
        kind = m.lastgroup or "OTHER"
        tok = m.group(0)
        if kind == "IDENT" and tok.lower().startswith("_cf_"):
            raise ValueError(f"internal column {tok!r} is not queryable in console SQL")
        if kind == "DQUOTE" and _unquote_sql_ident(tok).lower().startswith("_cf_"):
            raise ValueError(f"internal column {tok!r} is not queryable in console SQL")


def _rewrite_console_sql_with_canonicals(sql: str, *, svc_table: str, names: Mapping[str, str]) -> tuple[str, set[str]]:
    import secrets as _secrets

    _reject_internal_cf_identifiers(sql)
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

    def _find_matching_open_paren_lex(tokens: list[tuple[str, str]], close_s_pos: int, sig_list: list[int]) -> int:
        rev_pairs = {")": "(", "]": "[", "}": "{"}
        stack: list[str] = []
        for s_i in range(close_s_pos, -1, -1):
            tk, tt = tokens[sig_list[s_i]]
            if tk == "OP" and tt in rev_pairs:
                stack.append(rev_pairs[tt])
            elif tk == "OP" and tt in ("(", "[", "{"):
                if not stack or stack[-1] != tt:
                    return -1
                stack.pop()
                if not stack:
                    return s_i
        return -1

    def _is_kw_lex_pos(tokens: list[tuple[str, str]], s_idx: int, sig_list: list[int], *words: str) -> bool:
        if s_idx < 0 or s_idx >= len(sig_list):
            return False
        tok_idx = sig_list[s_idx]
        return (
            tokens[tok_idx][0] == "IDENT"
            and tokens[tok_idx][1].upper() in words
            and not _is_non_kw_lex_pos(tokens, tok_idx)
        )

    def _is_select_head_top_prefix_lex(tokens: list[tuple[str, str]], top_s_pos: int, sig_list: list[int]) -> bool:
        if top_s_pos < 1 or not _is_kw_lex_pos(tokens, top_s_pos, sig_list, "TOP"):
            return False
        if _is_kw_lex_pos(tokens, top_s_pos - 1, sig_list, "SELECT"):
            return True
        if top_s_pos >= 2 and _is_kw_lex_pos(tokens, top_s_pos - 1, sig_list, "ALL", "DISTINCT"):
            return _is_kw_lex_pos(tokens, top_s_pos - 2, sig_list, "SELECT")
        prev_tok = tokens[sig_list[top_s_pos - 1]]
        if prev_tok == ("OP", ")") or prev_tok[0] == "SUBQ":
            open_s = (
                top_s_pos - 1
                if prev_tok[0] == "SUBQ"
                else _find_matching_open_paren_lex(tokens, top_s_pos - 1, sig_list)
            )
            return (
                open_s >= 3
                and _is_kw_lex_pos(tokens, open_s - 1, sig_list, "ON")
                and _is_kw_lex_pos(tokens, open_s - 2, sig_list, "DISTINCT")
                and _is_kw_lex_pos(tokens, open_s - 3, sig_list, "SELECT")
            )
        return False

    def _is_select_head_signed_top_end_lex(tokens: list[tuple[str, str]], s_pos: int, sig_list: list[int]) -> bool:
        if s_pos < 2 or s_pos >= len(sig_list):
            return False
        return (
            tokens[sig_list[s_pos]][0] == "NUM"
            and tokens[sig_list[s_pos - 1]] in (("OP", "+"), ("OP", "-"))
            and _is_select_head_top_prefix_lex(tokens, s_pos - 2, sig_list)
        )

    def _is_select_head_top_end_lex(tokens: list[tuple[str, str]], s_pos: int, sig_list: list[int]) -> bool:
        if s_pos < 1 or s_pos >= len(sig_list):
            return False
        pk, pt = tokens[sig_list[s_pos]]
        if pk == "NUM":
            prev_s = s_pos - 1
            if prev_s >= 1 and tokens[sig_list[prev_s]] in (("OP", "+"), ("OP", "-")):
                prev_s -= 1
            return _is_select_head_top_prefix_lex(tokens, prev_s, sig_list)
        if (pk == "OP" and pt == ")") or pk == "SUBQ":
            open_s_pos = s_pos if pk == "SUBQ" else _find_matching_open_paren_lex(tokens, s_pos, sig_list)
            return open_s_pos >= 2 and _is_select_head_top_prefix_lex(tokens, open_s_pos - 1, sig_list)
        return False

    def _is_select_head_modifier_end_lex(tokens: list[tuple[str, str]], s_pos: int, sig_list: list[int]) -> bool:
        if s_pos < 1 or s_pos >= len(sig_list):
            return False
        if _is_select_head_top_end_lex(tokens, s_pos, sig_list):
            return True
        pk, pt = tokens[sig_list[s_pos]]
        if (pk == "OP" and pt == ")") or pk == "SUBQ":
            open_s_pos = s_pos if pk == "SUBQ" else _find_matching_open_paren_lex(tokens, s_pos, sig_list)
            return (
                open_s_pos >= 3
                and _is_kw_lex_pos(tokens, open_s_pos - 1, sig_list, "ON")
                and _is_kw_lex_pos(tokens, open_s_pos - 2, sig_list, "DISTINCT")
                and _is_kw_lex_pos(tokens, open_s_pos - 3, sig_list, "SELECT")
            )
        if (
            s_pos >= 3
            and _is_kw_lex_pos(tokens, s_pos, sig_list, "TIES")
            and _is_kw_lex_pos(tokens, s_pos - 1, sig_list, "WITH")
        ):
            return _is_select_head_top_end_lex(tokens, s_pos - 2, sig_list)
        return False

    def _is_postfix_factorial_lex(tokens: list[tuple[str, str]], s_pos: int, sig_list: list[int]) -> bool:
        return (
            0 < s_pos < len(sig_list)
            and tokens[sig_list[s_pos]] == ("OTHER", "!")
            and (
                _is_select_head_signed_top_end_lex(tokens, s_pos - 1, sig_list)
                or _is_postfix_factorial_lex(tokens, s_pos - 1, sig_list)
                or not _is_lex_on_operator_tok(tokens, s_pos - 1, sig_list)
            )
        )

    active_case_scans: set[tuple[int, int]] = set()

    def _has_matching_case_when_end_lex(tokens: list[tuple[str, str]], case_s_pos: int, sig_list: list[int]) -> bool:
        key = (len(tokens), case_s_pos)
        if key in active_case_scans:
            return False
        active_case_scans.add(key)
        try:
            delim_depth = 0
            case_depth = 1
            saw_top_when = False
            for k_s in range(case_s_pos + 1, len(sig_list)):
                tok_idx = sig_list[k_s]
                tk, tt = tokens[tok_idx]
                if tk == "OP" and tt in ("(", "[", "{"):
                    delim_depth += 1
                    continue
                if tk == "OP" and tt in (")", "]", "}"):
                    if delim_depth == 0:
                        return False
                    delim_depth -= 1
                    continue
                if delim_depth > 0:
                    continue
                if tk == "OP" and tt == ";":
                    return False
                if tk == "IDENT":
                    wu = tt.upper()
                    if (
                        wu in ("CASE", "WHEN", "END")
                        and not _is_non_kw_lex_pos(tokens, tok_idx)
                        and not _is_lex_dot_qualifier(tokens, k_s, sig_list)
                    ):
                        if wu == "CASE":
                            case_depth += 1
                        elif wu == "WHEN" and case_depth == 1:
                            saw_top_when = True
                        elif wu == "END":
                            case_depth -= 1
                            if case_depth == 0:
                                return saw_top_when
            return False
        finally:
            active_case_scans.discard(key)

    def _cannot_end_aliasable_lex(tokens: list[tuple[str, str]], pos: int | None, r: int, after_as_idx: int) -> bool:
        if pos is None or pos < 0 or _is_dot_or_dcolon_lex(tokens, pos):
            return True
        pk, pt = tokens[pos]
        if pk == "OP" and pt not in (")", "]", "}"):
            return True
        sig_list: list[int] | None = None
        s_pos = -1
        if pk == "OTHER":
            if pt != "!":
                return True
            if (r % 2) == 0:
                return False
            sig_list = _sig_indices(tokens)
            try:
                s_pos = sig_list.index(pos)
            except ValueError:
                return True
            return not _is_postfix_factorial_lex(tokens, s_pos, sig_list)
        if pk in ("NUM", "SUBQ") or (pk == "OP" and pt == ")") or (pk == "IDENT" and pt.upper() == "TIES"):
            sig_list = _sig_indices(tokens)
            try:
                s_pos = sig_list.index(pos)
            except ValueError:
                s_pos = -1
            if s_pos >= 0 and _is_select_head_modifier_end_lex(tokens, s_pos, sig_list):
                if (r % 2) == 0:
                    return False
                if r == 1 and _is_select_head_signed_top_end_lex(tokens, s_pos, sig_list):
                    after_sig_top = tokens[after_as_idx] if after_as_idx < len(tokens) else None
                    if (
                        after_sig_top is not None
                        and after_sig_top[0] == "IDENT"
                        and after_sig_top[1].upper() in ("WHERE", "SELECT")
                    ):
                        try:
                            after_s = sig_list.index(after_as_idx)
                        except ValueError:
                            after_s = -1
                        nxt_sig = (
                            tokens[sig_list[after_s + 1]] if 0 <= after_s and after_s + 1 < len(sig_list) else None
                        )
                        if (
                            nxt_sig is None
                            or nxt_sig in (("OP", ","), ("OP", ")"), ("OP", ";"))
                            or (
                                nxt_sig[0] == "IDENT"
                                and nxt_sig[1].upper()
                                in (
                                    "FROM",
                                    "WHERE",
                                    "GROUP",
                                    "HAVING",
                                    "ORDER",
                                    "LIMIT",
                                    "UNION",
                                    "INTERSECT",
                                    "EXCEPT",
                                )
                            )
                        ):
                            return False
                return True
        if pk != "IDENT" or _is_non_kw_lex_pos(tokens, pos):
            return False
        wu = pt.upper()
        after_sig = tokens[after_as_idx] if after_as_idx < len(tokens) else None
        if wu == "CASE":
            return after_sig is not None and after_sig[0] == "IDENT" and after_sig[1].upper() == "WHEN"
        if wu == "NOT":
            if r == 2 and (
                after_sig is None
                or after_sig in (("OP", ","), ("OP", ")"), ("OP", ";"))
                or (
                    after_sig[0] == "IDENT"
                    and (after_sig[1].upper() in _FROM_EXIT_KEYWORDS or after_sig[1].upper() in ("FROM", "JOIN"))
                )
            ):
                return False
            return True
        if wu == "INTERVAL":
            return (
                (r % 2) == 1
                and after_sig is not None
                and after_sig[0] == "IDENT"
                and after_sig[1].upper() in _SQL_INTERVAL_UNITS
            )
        if sig_list is None:
            sig_list = _sig_indices(tokens)
            try:
                s_pos = sig_list.index(pos)
            except ValueError:
                return False
        elif s_pos < 0:
            return False
        if _is_lex_prev_word_operator(tokens, s_pos, sig_list, after_sig):
            return True
        if wu in ("DISTINCT", "ALL"):
            return (
                s_pos >= 1
                and tokens[sig_list[s_pos - 1]][0] == "IDENT"
                and tokens[sig_list[s_pos - 1]][1].upper() in ("SELECT", "UNION", "INTERSECT", "EXCEPT")
                and not _is_non_kw_lex_pos(tokens, sig_list[s_pos - 1])
            )
        if wu in _AS_CLAUSE_OPENING_WORDS:
            if s_pos == 0:
                return True
            if _is_select_head_modifier_end_lex(tokens, s_pos - 1, sig_list):
                return False
            prev_idx = sig_list[s_pos - 1]
            prev_k, prev_t = tokens[prev_idx]
            if (prev_k == "OTHER" and not _is_postfix_factorial_lex(tokens, s_pos - 1, sig_list)) or (
                prev_k == "OP" and prev_t not in (")", "]", "}", ";")
            ):
                return False
            if prev_k == "IDENT" and not _is_non_kw_lex_pos(tokens, prev_idx):
                pw = prev_t.upper()
                if pw in ("SELECT", "FROM", "JOIN", "TABLE", "IN", "GLOBAL"):
                    return False
                if pw in ("DISTINCT", "ALL"):
                    is_setop_mod = (
                        s_pos >= 2
                        and tokens[sig_list[s_pos - 2]][0] == "IDENT"
                        and tokens[sig_list[s_pos - 2]][1].upper() in ("UNION", "INTERSECT", "EXCEPT")
                        and not _is_non_kw_lex_pos(tokens, sig_list[s_pos - 2])
                    )
                    if not is_setop_mod:
                        return False
                if _is_lex_prev_word_operator(tokens, s_pos - 1, sig_list, tokens[pos]):
                    is_bare_table_alias = (
                        s_pos >= 3
                        and tokens[sig_list[s_pos - 2]][0] in ("IDENT", "DQUOTE")
                        and tokens[sig_list[s_pos - 3]][0] == "IDENT"
                        and tokens[sig_list[s_pos - 3]][1].upper() in ("FROM", "JOIN")
                        and not _is_non_kw_lex_pos(tokens, sig_list[s_pos - 3])
                        and not _is_lex_distinct_from(tokens, s_pos - 3, sig_list)
                    )
                    if not is_bare_table_alias:
                        return False
            return True
        return False

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
        if _cannot_end_aliasable_lex(tokens, p, r, idx):
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
                or (tokens[sig_list[s_pos + 2]][0] == "NUM" and tokens[sig_list[s_pos + 2]][1][0].isdigit())
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

    def _is_lex_on_operator_tok(tokens: list[tuple[str, str]], s_pos: int, sig_list: list[int]) -> bool:
        if s_pos < 0:
            return True
        if _is_select_head_modifier_end_lex(tokens, s_pos, sig_list):
            return True
        tok_idx = sig_list[s_pos]
        tk, tt = tokens[tok_idx]
        if tk == "OTHER":
            return not _is_postfix_factorial_lex(tokens, s_pos, sig_list)
        if tk == "OP" and tt not in (")", "]", "}"):
            return True
        if tk != "IDENT" or _is_non_kw_lex_pos(tokens, tok_idx):
            return False
        wu = tt.upper()
        if wu in ("SELECT", "WHERE", "PREWHERE", "HAVING", "QUALIFY"):
            if s_pos == 0:
                return True
            prev_k, prev_t = tokens[sig_list[s_pos - 1]]
            if (prev_k == "OTHER" and not _is_postfix_factorial_lex(tokens, s_pos - 1, sig_list)) or (
                prev_k == "OP" and prev_t not in (")", "]", "}", ";")
            ):
                return False
            if prev_k == "IDENT" and _is_lex_prev_word_operator(tokens, s_pos - 1, sig_list, tokens[tok_idx]):
                return False
            return True
        if wu in ("DISTINCT", "ALL"):
            return (
                s_pos >= 1
                and tokens[sig_list[s_pos - 1]][0] == "IDENT"
                and tokens[sig_list[s_pos - 1]][1].upper() in ("SELECT", "UNION", "INTERSECT", "EXCEPT")
                and not _is_non_kw_lex_pos(tokens, sig_list[s_pos - 1])
            ) or (
                wu == "ALL"
                and s_pos + 1 < len(sig_list)
                and (tokens[sig_list[s_pos + 1]] == ("OP", "(") or tokens[sig_list[s_pos + 1]][0] == "SUBQ")
            )
        after_operand = tokens[sig_list[s_pos + 2]] if s_pos + 2 < len(sig_list) else None
        return _is_lex_prev_word_operator(tokens, s_pos, sig_list, after_operand)

    def _is_lex_prev_word_operator(
        tokens: list[tuple[str, str]], prev_s_pos: int, sig_list: list[int], nxt_sig: tuple[str, str] | None
    ) -> bool:
        prev_idx = sig_list[prev_s_pos]
        pw = tokens[prev_idx][1].upper()
        if pw not in _ON_EXPR_PREV_WORDS or _is_non_kw_lex_pos(tokens, prev_idx):
            return False
        if pw == "FROM":
            return _is_lex_distinct_from(tokens, prev_s_pos, sig_list)
        if pw == "TO":
            return (
                prev_s_pos >= 1
                and tokens[sig_list[prev_s_pos - 1]][0] == "IDENT"
                and tokens[sig_list[prev_s_pos - 1]][1].upper() == "SIMILAR"
                and not _is_non_kw_lex_pos(tokens, sig_list[prev_s_pos - 1])
            )
        if pw == "ZONE":
            return (
                prev_s_pos >= 2
                and tokens[sig_list[prev_s_pos - 1]][0] == "IDENT"
                and tokens[sig_list[prev_s_pos - 1]][1].upper() == "TIME"
                and not _is_non_kw_lex_pos(tokens, sig_list[prev_s_pos - 1])
                and tokens[sig_list[prev_s_pos - 2]][0] == "IDENT"
                and tokens[sig_list[prev_s_pos - 2]][1].upper() == "AT"
                and not _is_non_kw_lex_pos(tokens, sig_list[prev_s_pos - 2])
            )
        if pw == "INTERVAL":
            return (
                nxt_sig is not None
                and nxt_sig[0] == "IDENT"
                and nxt_sig[1].upper() in _SQL_INTERVAL_UNITS
                and prev_s_pos + 2 < len(sig_list)
                and not _is_non_kw_lex_pos(tokens, sig_list[prev_s_pos + 2])
            )
        if pw == "ESCAPE":
            return prev_s_pos >= 1 and not _is_lex_on_operator_tok(tokens, prev_s_pos - 1, sig_list)
        if pw in ("SYMMETRIC", "ASYMMETRIC"):
            return (
                prev_s_pos >= 1
                and tokens[sig_list[prev_s_pos - 1]][0] == "IDENT"
                and tokens[sig_list[prev_s_pos - 1]][1].upper() == "BETWEEN"
                and not _is_non_kw_lex_pos(tokens, sig_list[prev_s_pos - 1])
            )
        if pw == "NOT":
            return True
        if pw == "CASE":
            if not _has_matching_case_when_end_lex(tokens, prev_s_pos, sig_list):
                return False
            first_after = tokens[sig_list[prev_s_pos + 1]] if prev_s_pos + 1 < len(sig_list) else None
            if first_after is None:
                return False
            if (
                first_after[0] == "IDENT"
                and first_after[1].upper() == "WHEN"
                and not _is_non_kw_lex_pos(tokens, sig_list[prev_s_pos + 1])
            ):
                return True
            if first_after[0] != "IDENT":
                return True
            if nxt_sig is None:
                return False
            if (
                nxt_sig in (("OP", "."), ("OP", "("), ("OP", "["))
                or nxt_sig[0] in ("OTHER", "SUBQ")
                or (nxt_sig[0] == "OP" and nxt_sig[1] not in ("(", ")", "[", "]", "{", "}", ",", ";"))
            ):
                return True
            return (
                nxt_sig[0] == "IDENT"
                and nxt_sig[1].upper() in _ON_EXPR_NEXT_WORDS
                and (prev_s_pos + 2 >= len(sig_list) or not _is_non_kw_lex_pos(tokens, sig_list[prev_s_pos + 2]))
            )
        if pw == "ON":
            return prev_s_pos == 0 or not _is_lex_on_operator_tok(tokens, prev_s_pos - 1, sig_list)
        if pw == "WHEN":
            if (
                prev_s_pos >= 1
                and tokens[sig_list[prev_s_pos - 1]][0] == "IDENT"
                and tokens[sig_list[prev_s_pos - 1]][1].upper() == "CASE"
            ):
                return not _is_non_kw_lex_pos(tokens, sig_list[prev_s_pos - 1])
            return prev_s_pos >= 1 and not _is_lex_on_operator_tok(tokens, prev_s_pos - 1, sig_list)
        if (
            pw in ("IN", "LIKE", "ILIKE", "RLIKE", "REGEXP", "GLOB", "SIMILAR", "BETWEEN")
            and prev_s_pos >= 2
            and tokens[sig_list[prev_s_pos - 1]][0] == "IDENT"
            and tokens[sig_list[prev_s_pos - 1]][1].upper() == "NOT"
            and not _is_non_kw_lex_pos(tokens, sig_list[prev_s_pos - 1])
        ):
            return not _is_lex_on_operator_tok(tokens, prev_s_pos - 2, sig_list)
        return prev_s_pos >= 1 and not _is_lex_on_operator_tok(tokens, prev_s_pos - 1, sig_list)

    def _is_lex_multi_token_post_on_exit(tokens: list[tuple[str, str]], s_pos: int, sig_list: list[int]) -> bool:
        if _is_lex_within_group(tokens, s_pos, sig_list):
            return False
        wu = tokens[sig_list[s_pos]][1].upper()
        nxt_sig = tokens[sig_list[s_pos + 1]] if s_pos + 1 < len(sig_list) else None
        if nxt_sig is None:
            return False
        nxt_is_kw = nxt_sig[0] == "IDENT" and not _is_non_kw_lex_pos(tokens, sig_list[s_pos + 1])
        if wu in ("GROUP", "ORDER"):
            return nxt_is_kw and nxt_sig[1].upper() in ("BY", "ALL")
        if wu == "FETCH":
            return nxt_is_kw and nxt_sig[1].upper() in ("FIRST", "NEXT")
        if wu == "INTO":
            return nxt_is_kw and nxt_sig[1].upper() == "OUTFILE"
        if wu in ("UNION", "INTERSECT", "EXCEPT"):
            if nxt_sig == ("OP", "(") or nxt_sig[0] == "SUBQ":
                return True
            return (
                nxt_is_kw
                and not _is_lex_dot_qualifier(tokens, s_pos + 1, sig_list)
                and nxt_sig[1].upper() in ("ALL", "DISTINCT", "BY", "SELECT", "WITH", "FROM", "VALUES", "TABLE")
            )
        return False

    def _is_lex_on_expr_ident(tokens: list[tuple[str, str]], s_pos: int, sig_list: list[int]) -> bool:
        if _is_lex_dot_qualifier(tokens, s_pos, sig_list):
            return True
        if _is_lex_within_group(tokens, s_pos, sig_list):
            return True
        if _is_lex_multi_token_post_on_exit(tokens, s_pos, sig_list):
            return False
        wu = tokens[sig_list[s_pos]][1].upper()
        prev_idx = sig_list[s_pos - 1] if s_pos > 0 else None
        prev_sig = tokens[prev_idx] if prev_idx is not None else None
        nxt_sig = tokens[sig_list[s_pos + 1]] if s_pos + 1 < len(sig_list) else None
        if prev_sig is not None:
            if (prev_sig[0] == "OTHER" and not _is_postfix_factorial_lex(tokens, s_pos - 1, sig_list)) or (
                prev_sig[0] == "OP" and prev_sig[1] not in (")", "]", "}", ";")
            ):
                return True
            if prev_sig[0] == "IDENT" and _is_lex_prev_word_operator(tokens, s_pos - 1, sig_list, nxt_sig):
                return True
        if nxt_sig is not None:
            if nxt_sig[0] == "OTHER":
                if wu in ("SELECT", "WHERE", "PREWHERE", "HAVING", "QUALIFY", "LIMIT", "OFFSET") and nxt_sig[1] in (
                    "~",
                    "!",
                ):
                    return False
                return True
            if nxt_sig == ("OP", "[") and wu not in (
                "SELECT",
                "WHERE",
                "PREWHERE",
                "HAVING",
                "QUALIFY",
                "LIMIT",
                "OFFSET",
            ):
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
                and not _is_non_kw_lex_pos(tokens, sig_list[s_pos + 1])
                and wu not in ("SELECT", "WHERE", "PREWHERE", "HAVING", "QUALIFY", "LIMIT", "OFFSET")
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

    def _coerce_request_predicates(
        segment_tokens: list[tuple[str, str]],
        *,
        touches_request: bool = True,
        derived_str_cols: frozenset[str] = frozenset(),
        derived_bool_cols: frozenset[str] = frozenset(),
        alias_schema_map: Mapping[str, _DerivedCfSchema] | None = None,
        non_cf_shadowed_str_cols: frozenset[str] = frozenset(),
        non_cf_shadowed_bool_cols: frozenset[str] = frozenset(),
        unqualified_derived: bool = False,
    ) -> str:
        lit_nonce = _secrets.token_hex(8)
        saved_lits: list[str] = []
        parts: list[str] = []
        sig_idxs = _sig_indices(segment_tokens)
        sig_pos_map = {idx: pos for pos, idx in enumerate(sig_idxs)}
        for idx, (k, t) in enumerate(segment_tokens):
            if k == "COMMENT":
                s_idx = len(saved_lits)
                saved_lits.append(t)
                parts.append(f"__fla_str_lit_{lit_nonce}_{s_idx}__")
            elif k == "DQUOTE":
                s_pos = sig_pos_map.get(idx, -1)
                next_sig = segment_tokens[sig_idxs[s_pos + 1]] if (s_pos >= 0 and s_pos + 1 < len(sig_idxs)) else None
                if next_sig == ("OP", "."):
                    parts.append(t)
                else:
                    s_idx = len(saved_lits)
                    saved_lits.append(t)
                    parts.append(f"__fla_str_lit_{lit_nonce}_{s_idx}__")
            elif k == "STR":
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
        from backend.core.sql_validator import SAFE_IDENT_RE

        _single_ident_re = rf'(?:{SAFE_IDENT_RE.pattern}|`(?:``|[^`])+`|"(?:""|[^"])+")'
        _qual_re = rf"(?:{_single_ident_re}\s*\.\s*)*"
        _bool_rules = (
            (r"IN\s*\(\s*'true'\s*\)", "IN ('true', '1')"),
            (r"NOT\s+IN\s*\(\s*'true'\s*\)", "NOT IN ('true', '1')"),
            (r"IN\s*\(\s*'false'\s*\)", "IN ('false', '0', '')"),
            (r"NOT\s+IN\s*\(\s*'false'\s*\)", "IN ('true', '1')"),
            (r"IS\s+NOT\s+TRUE\b", "NOT IN ('true', '1')"),
            (r"IS\s+TRUE\b", "IN ('true', '1')"),
            (r"IS\s+NOT\s+FALSE\b", "NOT IN ('false', '0')"),
            (r"IS\s+FALSE\b", "IN ('false', '0')"),
            (r"(?:!=|<>)\s*'true'", "NOT IN ('true', '1')"),
            (r"(?:!=|<>)\s*'false'", "IN ('true', '1')"),
            (r"(?:!=|<>)\s*\btrue\b", "NOT IN ('true', '1')"),
            (r"(?:!=|<>)\s*\bfalse\b", "IN ('true', '1')"),
            (r"(?<![!<>])=\s*'true'", "IN ('true', '1')"),
            (r"(?<![!<>])=\s*'false'", "IN ('false', '0', '')"),
            (r"(?<![!<>])=\s*\btrue\b", "IN ('true', '1')"),
            (r"(?<![!<>])=\s*\bfalse\b", "IN ('false', '0', '')"),
        )
        _rev_bool_rules = (
            (r"(?<![A-Za-z0-9_.\"'`])(?:'true'|\btrue\b)\s*(?:!=|<>)", "NOT IN ('true', '1')"),
            (r"(?<![A-Za-z0-9_.\"'`])(?:'false'|\bfalse\b)\s*(?:!=|<>)", "IN ('true', '1')"),
            (r"(?<![A-Za-z0-9_.\"'`])(?:'true'|\btrue\b)\s*(?<![!<>])=", "IN ('true', '1')"),
            (r"(?<![A-Za-z0-9_.\"'`])(?:'false'|\bfalse\b)\s*(?<![!<>])=", "IN ('false', '0', '')"),
        )
        _unary_kw = frozenset(
            {
                "WHERE",
                "AND",
                "OR",
                "NOT",
                "XOR",
                "ON",
                "WHEN",
                "THEN",
                "ELSE",
                "HAVING",
                "PREWHERE",
                "SELECT",
                "CASE",
                "BY",
            }
        )

        def _is_standalone_num(src: str, start_idx: int, num_tok: str) -> bool:
            if not num_tok.startswith("-"):
                return True
            before = src[:start_idx].rstrip()
            if not before:
                return True
            if before[-1] in ")]\"'`":
                return False
            if before[-1].isalnum() or before[-1] == "_":
                m_w = re.search(rf"({SAFE_IDENT_RE.pattern})$", before)
                return bool(m_w and m_w.group(1).upper() in _unary_kw)
            return True

        if touches_request:
            for b_field in sorted(_REQUEST_BOOLEAN_CF_FIELDS):
                for pat, repl in _bool_rules:
                    seg_sql = re.sub(
                        rf"({_qual_re})custom_fields\['{re.escape(b_field)}'\]\s*{pat}",
                        rf"lower(\1custom_fields['{b_field}']) {repl}",
                        seg_sql,
                        flags=re.IGNORECASE,
                    )
                for r_pat, r_repl in _rev_bool_rules:
                    seg_sql = re.sub(
                        rf"{r_pat}\s*({_qual_re})custom_fields\['{re.escape(b_field)}'\]",
                        rf"lower(\1custom_fields['{b_field}']) {r_repl}",
                        seg_sql,
                        flags=re.IGNORECASE,
                    )
            for pat, repl in (
                (
                    rf"({_qual_re}custom_fields\['[a-zA-Z0-9_]+'\])\s*(=|!=|<>)\s*(-?\d+(?:\.\d+)?)\b",
                    r"\1 \2 '\3'",
                ),
                (
                    rf"({_qual_re}custom_fields\['[a-zA-Z0-9_]+'\])\s*(<=|>=|<|>)\s*(-?\d+(?:\.\d+)?)\b",
                    r"toFloat64OrZero(\1) \2 \3",
                ),
                (
                    rf"({_qual_re}custom_fields\['[a-zA-Z0-9_]+'\])\s*((?:NOT\s+)?BETWEEN)\s+(-?\d+(?:\.\d+)?)(?![A-Za-z0-9_.])\s+AND\s+(-?\d+(?:\.\d+)?)(?![A-Za-z0-9_.])",
                    r"toFloat64OrZero(\1) \2 \3 AND \4",
                ),
            ):
                seg_sql = re.sub(pat, repl, seg_sql, flags=re.IGNORECASE)

            def _sub_req_rev_eq(m: re.Match[str]) -> str:
                if not _is_standalone_num(m.string, m.start(), m.group(1)):
                    return m.group(0)
                return f"'{m.group(1)}' {m.group(2)} {m.group(3)}"

            seg_sql = re.sub(
                rf"(?<![A-Za-z0-9_.\"'`])(-?\d+(?:\.\d+)?)\s*(=|!=|<>)\s*({_qual_re}custom_fields\['[a-zA-Z0-9_]+'\])",
                _sub_req_rev_eq,
                seg_sql,
            )

            def _sub_req_rev_cmp(m: re.Match[str]) -> str:
                if not _is_standalone_num(m.string, m.start(), m.group(1)):
                    return m.group(0)
                return f"{m.group(1)} {m.group(2)} toFloat64OrZero({m.group(3)})"

            seg_sql = re.sub(
                rf"(?<![A-Za-z0-9_.\"'`])(-?\d+(?:\.\d+)?)\s*(<=|>=|<|>)\s*({_qual_re}custom_fields\['[a-zA-Z0-9_]+'\])",
                _sub_req_rev_cmp,
                seg_sql,
            )

        _alias_map = alias_schema_map or {}
        if derived_str_cols and (unqualified_derived or _alias_map):
            _physical_req_cols = _REQUEST_TYPED_COLUMNS | {
                "timestamp",
                "event_timestamp",
                "ip",
                "client_ip",
                "custom_fields",
            }

            def _qual_matches_derived(raw_qual: str, col_name: str, *, want_bool: bool = False) -> bool:
                if not raw_qual:
                    if not unqualified_derived:
                        return False
                    if touches_request and col_name in _physical_req_cols:
                        return False
                    if want_bool and col_name in non_cf_shadowed_bool_cols:
                        return False
                    if not want_bool and col_name in non_cf_shadowed_str_cols:
                        return False
                    return True
                q_tok = _unquote_sql_ident(raw_qual.rstrip()[:-1].strip()).lower()
                q_sch = _alias_map.get(q_tok)
                if q_sch is None:
                    return False
                if want_bool:
                    return col_name in q_sch.bool_cols
                return col_name in q_sch.str_cols

            for d_col in sorted(derived_str_cols):
                col_pat = rf"(?<![A-Za-z0-9_.\"'`])({_single_ident_re}\s*\.\s*)?(\b{re.escape(d_col)}\b)(?!\s*[.(])"
                if d_col in _REQUEST_BOOLEAN_CF_FIELDS or d_col in derived_bool_cols:
                    for b_pat, b_repl in _bool_rules:

                        def _sub_bool(m: re.Match[str], _repl: str = b_repl, _dc: str = d_col) -> str:
                            q_part = m.group(1) or ""
                            if not _qual_matches_derived(q_part, _dc, want_bool=True):
                                return m.group(0)
                            return f"lower({q_part}{m.group(2)}) {_repl}"

                        seg_sql = re.sub(rf"{col_pat}\s*{b_pat}", _sub_bool, seg_sql, flags=re.IGNORECASE)
                    for rb_pat, rb_repl in _rev_bool_rules:

                        def _sub_rev_bool(m: re.Match[str], _repl: str = rb_repl, _dc: str = d_col) -> str:
                            q_part = m.group(1) or ""
                            if not _qual_matches_derived(q_part, _dc, want_bool=True):
                                return m.group(0)
                            return f"lower({q_part}{m.group(2)}) {_repl}"

                        seg_sql = re.sub(rf"{rb_pat}\s*{col_pat}", _sub_rev_bool, seg_sql, flags=re.IGNORECASE)

                def _sub_eq(m: re.Match[str], _dc: str = d_col) -> str:
                    q_part = m.group(1) or ""
                    if not _qual_matches_derived(q_part, _dc):
                        return m.group(0)
                    return f"{q_part}{m.group(2)} {m.group(3)} '{m.group(4)}'"

                seg_sql = re.sub(
                    rf"{col_pat}\s*(=|!=|<>)\s*(-?\d+(?:\.\d+)?)\b",
                    _sub_eq,
                    seg_sql,
                    flags=re.IGNORECASE,
                )

                def _sub_rev_eq(m: re.Match[str], _dc: str = d_col) -> str:
                    q_part = m.group(3) or ""
                    if not _is_standalone_num(m.string, m.start(), m.group(1)) or not _qual_matches_derived(
                        q_part, _dc
                    ):
                        return m.group(0)
                    return f"'{m.group(1)}' {m.group(2)} {q_part}{m.group(4)}"

                seg_sql = re.sub(
                    rf"(?<![A-Za-z0-9_.\"'`])(-?\d+(?:\.\d+)?)\s*(=|!=|<>)\s*{col_pat}",
                    _sub_rev_eq,
                    seg_sql,
                    flags=re.IGNORECASE,
                )

                def _sub_cmp(m: re.Match[str], _dc: str = d_col) -> str:
                    q_part = m.group(1) or ""
                    if not _qual_matches_derived(q_part, _dc):
                        return m.group(0)
                    return f"toFloat64OrZero({q_part}{m.group(2)}) {m.group(3)} {m.group(4)}"

                seg_sql = re.sub(
                    rf"{col_pat}\s*(<=|>=|<|>)\s*(-?\d+(?:\.\d+)?)\b",
                    _sub_cmp,
                    seg_sql,
                    flags=re.IGNORECASE,
                )

                def _sub_rev_cmp(m: re.Match[str], _dc: str = d_col) -> str:
                    q_part = m.group(3) or ""
                    if not _is_standalone_num(m.string, m.start(), m.group(1)) or not _qual_matches_derived(
                        q_part, _dc
                    ):
                        return m.group(0)
                    return f"{m.group(1)} {m.group(2)} toFloat64OrZero({q_part}{m.group(4)})"

                seg_sql = re.sub(
                    rf"(?<![A-Za-z0-9_.\"'`])(-?\d+(?:\.\d+)?)\s*(<=|>=|<|>)\s*{col_pat}",
                    _sub_rev_cmp,
                    seg_sql,
                    flags=re.IGNORECASE,
                )

                def _sub_between(m: re.Match[str], _dc: str = d_col) -> str:
                    q_part = m.group(1) or ""
                    if not _qual_matches_derived(q_part, _dc):
                        return m.group(0)
                    return f"toFloat64OrZero({q_part}{m.group(2)}) {m.group(3)} {m.group(4)} AND {m.group(5)}"

                seg_sql = re.sub(
                    rf"{col_pat}\s+((?:NOT\s+)?BETWEEN)\s+(-?\d+(?:\.\d+)?)(?![A-Za-z0-9_.])\s+AND\s+(-?\d+(?:\.\d+)?)(?![A-Za-z0-9_.])",
                    _sub_between,
                    seg_sql,
                    flags=re.IGNORECASE,
                )

        if saved_lits:
            seg_sql = re.sub(rf"__fla_str_lit_{lit_nonce}_(\d+)__", lambda m: saved_lits[int(m.group(1))], seg_sql)
        return seg_sql

    @dataclass(frozen=True)
    class _DerivedCfSchema:
        str_cols: frozenset[str] = frozenset()
        bool_cols: frozenset[str] = frozenset()
        positional: tuple[tuple[bool, bool], ...] | None = None
        all_cols: frozenset[str] = frozenset()

    def _demote_col_list_domains(doms: set[str]) -> set[str]:
        out: set[str] = set()
        for dom in doms:
            if dom == "request":
                out.add("request_derived")
            elif dom in ("rum_vitals", "rum_errors"):
                out.add("rum_derived")
            else:
                out.add(dom)
        return out

    def _remap_derived_cf_schema(schema: _DerivedCfSchema, col_names: list[str]) -> _DerivedCfSchema:
        keep = frozenset(col_names)
        if schema.positional is not None:
            new_str: set[str] = set()
            new_bool: set[str] = set()
            new_pos: list[tuple[bool, bool]] = []
            for idx_c, c_name in enumerate(col_names):
                is_s, is_b = schema.positional[idx_c] if idx_c < len(schema.positional) else (False, False)
                if is_s:
                    new_str.add(c_name)
                if is_b:
                    new_bool.add(c_name)
                new_pos.append((is_s, is_b))
            return _DerivedCfSchema(frozenset(new_str), frozenset(new_bool), tuple(new_pos), keep)
        return _DerivedCfSchema(frozenset(), frozenset(), None, keep)

    def _rewrite_tokens(
        tokens: list[tuple[str, str]],
        inherited_ctes: dict[str, tuple[set[str], _DerivedCfSchema]],
        *,
        is_subquery: bool = False,
    ) -> tuple[str, set[str], set[str], _DerivedCfSchema]:
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
            return _wrap("".join(t for _, t in tokens)), set(), set(), _DerivedCfSchema()

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
                if (
                    p >= len(tokens)
                    or tokens[p][0] not in ("IDENT", "DQUOTE")
                    or (tokens[p][0] == "IDENT" and tokens[p][1].upper() == "SELECT")
                ):
                    break
                cte_name = _unquote_sql_ident(tokens[p][1])
                p2 = p + 1
                while p2 < len(tokens) and tokens[p2][0] in ("WS", "COMMENT"):
                    p2 += 1
                explicit_cte_cols: list[str] | None = None
                if p2 < len(tokens) and tokens[p2] == ("OP", "("):
                    rp_cols = _find_matching_paren(tokens, p2)
                    if not _is_valid_lex_ident_list(tokens[p2 + 1 : rp_cols]):
                        break
                    explicit_cte_cols = [
                        _unquote_sql_ident(tk).lower()
                        for kd, tk in tokens[p2 + 1 : rp_cols]
                        if kd in ("IDENT", "DQUOTE")
                    ]
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
                inner_sql, inner_domains, inner_canonicals, inner_schema = _rewrite_tokens(
                    tokens[p2 + 1 : close_idx], ctes, is_subquery=True
                )
                if explicit_cte_cols is not None:
                    inner_domains = _demote_col_list_domains(inner_domains)
                    inner_schema = _remap_derived_cf_schema(inner_schema, explicit_cte_cols)
                ctes[cte_name.lower()] = (inner_domains, inner_schema)
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
                    return _wrap(out_prefix), set(), all_canonicals, _DerivedCfSchema()

        top_sig = _sig_indices(tokens)
        top_sig_pos = {idx: pos for pos, idx in enumerate(top_sig)}
        branches: list[tuple[list[tuple[str, str]], list[tuple[str, str]]]] = []
        cur_branch: list[tuple[str, str]] = []
        i_tok = 0
        depth = 0
        split_in_from = False
        split_expect_table = False
        split_in_on = False
        while i_tok < len(tokens):
            k, t = tokens[i_tok]
            if k == "OP" and t in ("(", "[", "{"):
                if depth == 0 and split_in_from and split_expect_table:
                    split_expect_table = False
                depth += 1
            elif k == "OP" and t in (")", "]", "}"):
                depth = max(0, depth - 1)
            elif depth == 0 and k == "OP" and t == "," and split_in_from:
                split_expect_table = True
                split_in_on = False
            elif depth == 0 and k == "IDENT" and not _is_non_kw_lex_pos(tokens, i_tok):
                wu_top = t.upper()
                s_pos_top = top_sig_pos[i_tok]
                is_top_dot = _is_lex_dot_qualifier(tokens, s_pos_top, top_sig)
                is_top_within = _is_lex_within_group(tokens, s_pos_top, top_sig)
                is_top_on_ident = (
                    split_in_on
                    and not split_expect_table
                    and (wu_top in _FROM_EXIT_KEYWORDS or wu_top in ("SELECT", "FROM", "JOIN", "TABLE"))
                    and _is_lex_on_expr_ident(tokens, s_pos_top, top_sig)
                )
                if not is_top_dot and not is_top_within and not is_top_on_ident:
                    if wu_top in ("UNION", "INTERSECT", "EXCEPT") and _is_lex_multi_token_post_on_exit(
                        tokens, s_pos_top, top_sig
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
                        split_in_from = False
                        split_expect_table = False
                        split_in_on = False
                        i_tok = j
                        continue
                    if (wu_top == "FROM" and not _is_lex_distinct_from(tokens, s_pos_top, top_sig)) or wu_top == "JOIN":
                        split_in_from = True
                        split_expect_table = True
                        split_in_on = False
                    elif wu_top in _JOIN_MODIFIER_KEYWORDS:
                        if split_in_from:
                            split_expect_table = False
                    elif split_in_from and not split_expect_table and not split_in_on and wu_top == "SELECT":
                        after_top = tokens[top_sig[s_pos_top + 1]] if s_pos_top + 1 < len(top_sig) else None
                        is_fn_proj_top = (
                            after_top is not None
                            and after_top[0] == "IDENT"
                            and after_top[1].upper() in _FROM_FIRST_SELECT_FN_KEYWORDS
                            and s_pos_top + 2 < len(top_sig)
                            and tokens[top_sig[s_pos_top + 2]] == ("OP", "(")
                        )
                        is_bare_sel_top = not is_fn_proj_top and (
                            after_top is None
                            or after_top == ("OP", ",")
                            or (
                                after_top[0] == "IDENT"
                                and not _is_lex_dot_qualifier(tokens, s_pos_top + 1, top_sig)
                                and after_top[1].upper() in (_SQL_CLAUSE_STOPWORDS - {"ALL"})
                            )
                        )
                        if not is_bare_sel_top:
                            split_in_from = False
                            split_in_on = False
                    elif (
                        split_in_from
                        and not split_expect_table
                        and not split_in_on
                        and wu_top
                        in (
                            "PREWHERE",
                            "SAMPLE",
                            "SETTINGS",
                            "FORMAT",
                            "FOR",
                            "SET",
                        )
                    ):
                        after_top = tokens[top_sig[s_pos_top + 1]] if s_pos_top + 1 < len(top_sig) else None
                        is_bare_top = (
                            after_top is None
                            or after_top in (("OP", ","), ("OP", "("))
                            or (after_top[0] == "IDENT" and after_top[1].upper() in _SQL_CLAUSE_STOPWORDS)
                        )
                        if not is_bare_top and wu_top != "SAMPLE":
                            split_in_from = False
                            split_in_on = False
                    elif (
                        wu_top in _FROM_EXIT_KEYWORDS or (split_in_from and wu_top == "SELECT")
                    ) and _can_exit_from_lex(tokens, s_pos_top, top_sig, in_on_clause=split_in_on):
                        split_in_from = False
                        split_expect_table = False
                        split_in_on = False
                    elif split_in_from and (wu_top == "ON" or (wu_top == "USING" and not split_in_on)):
                        split_expect_table = False
                        split_in_on = wu_top == "ON"
                    elif split_in_from and not split_in_on and wu_top in ("FINAL", "SAMPLE"):
                        split_expect_table = False
                    elif split_in_from and split_expect_table and wu_top not in ("LATERAL", "ONLY"):
                        split_expect_table = False
            elif depth == 0 and split_in_from and split_expect_table and k in ("DQUOTE", "STR"):
                split_expect_table = False
            cur_branch.append(tokens[i_tok])
            i_tok += 1
        branches.append((cur_branch, []))

        all_domains: set[str] = set()
        all_str_cf_cols: set[str] = set()
        all_bool_cf_cols: set[str] = set()
        all_known_cols: set[str] = set()
        merged_positional: tuple[tuple[bool, bool], ...] | None = None
        first_branch = True
        rendered_parts: list[str] = [out_prefix]
        for b_tokens, sep_tokens in branches:
            if _has_enclosing_parens(b_tokens) is not None:
                b_sql, b_domains, b_canonicals, b_schema = _rewrite_tokens(b_tokens, ctes, is_subquery=is_subquery)
            else:
                b_sql, b_domains, b_canonicals, b_schema = _rewrite_single_branch(
                    b_tokens, ctes, is_subquery=is_subquery
                )
            all_domains |= b_domains
            all_canonicals |= b_canonicals
            all_str_cf_cols |= b_schema.str_cols
            all_bool_cf_cols |= b_schema.bool_cols
            if first_branch:
                all_known_cols = set(b_schema.all_cols)
                merged_positional = b_schema.positional
                first_branch = False
            elif (
                merged_positional is not None
                and b_schema.positional is not None
                and len(merged_positional) == len(b_schema.positional)
            ):
                merged_positional = tuple(
                    (a_s or b_s, a_b or b_b)
                    for (a_s, a_b), (b_s, b_b) in zip(merged_positional, b_schema.positional, strict=True)
                )
            else:
                merged_positional = None
            rendered_parts.append(b_sql)
            rendered_parts.extend(tk for _, tk in sep_tokens)
        return (
            _wrap("".join(rendered_parts)),
            all_domains,
            all_canonicals,
            _DerivedCfSchema(
                frozenset(all_str_cf_cols),
                frozenset(all_bool_cf_cols),
                merged_positional,
                frozenset(all_known_cols),
            ),
        )

    def _rewrite_single_branch(
        tokens: list[tuple[str, str]],
        ctes: dict[str, tuple[set[str], _DerivedCfSchema]],
        *,
        is_subquery: bool = False,
    ) -> tuple[str, set[str], set[str], _DerivedCfSchema]:
        domains: set[str] = set()
        branch_canonicals: set[str] = set()
        alias_domains: dict[str, set[str]] = {}
        alias_schema: dict[str, _DerivedCfSchema] = {}
        branch_inherited_str_cf: set[str] = set()
        branch_inherited_bool_cf: set[str] = set()
        branch_inherited_all_cols: set[str] = set()
        processed: list[tuple[str, str]] = []
        subquery_placeholders: dict[str, str] = {}
        subquery_domains: dict[str, set[str]] = {}
        subquery_schema: dict[str, _DerivedCfSchema] = {}
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
                    sub_sql, sub_doms, sub_canons, sub_sch = _rewrite_tokens(inner, ctes, is_subquery=True)
                    branch_canonicals |= sub_canons
                    ph = f"__fla_subq_{sub_nonce}_{len(subquery_placeholders)}__"
                    subquery_placeholders[ph] = f"({sub_sql})"
                    subquery_domains[ph] = sub_doms
                    subquery_schema[ph] = sub_sch
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

        def _record_table_alias(a_idx: int, item_doms: set[str], item_sch: _DerivedCfSchema) -> None:
            table_alias_idxs.add(a_idx)
            raw_alias = processed[a_idx][1]
            unq_low = _unquote_sql_ident(raw_alias).lower()
            alias_domains[raw_alias.lower()] = item_doms
            alias_domains[unq_low] = item_doms
            alias_schema[raw_alias.lower()] = item_sch
            alias_schema[unq_low] = item_sch

        def _consume_optional_col_alias_list(next_s_pos: int) -> tuple[list[str] | None, int]:
            if next_s_pos < len(sig_idxs) and processed[sig_idxs[next_s_pos]] == ("OP", "("):
                lp_idx = sig_idxs[next_s_pos]
                rp_idx = _find_matching_paren(processed, lp_idx)
                if _is_valid_lex_ident_list(processed[lp_idx + 1 : rp_idx]):
                    col_names = [
                        _unquote_sql_ident(tk).lower()
                        for kd, tk in processed[lp_idx + 1 : rp_idx]
                        if kd in ("IDENT", "DQUOTE")
                    ]
                    for c_tok_idx in range(lp_idx + 1, rp_idx):
                        if processed[c_tok_idx][0] in ("IDENT", "DQUOTE"):
                            table_alias_idxs.add(c_tok_idx)
                    end_s_pos = next_s_pos + 1
                    while end_s_pos < len(sig_idxs) and sig_idxs[end_s_pos] <= rp_idx:
                        end_s_pos += 1
                    return col_names, end_s_pos
            return None, next_s_pos

        table_token_idxs: dict[int, str] = {}
        col_list_table_spans: dict[int, tuple[int, str]] = {}
        table_alias_idxs: set[int] = set()
        from_source_schemas: list[_DerivedCfSchema] = []
        func_stack: list[str] = []
        top_from_s_pos: int | None = None
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

            if k == "IDENT" and t.upper() == "IN" and not _is_non_kw_lex_pos(processed, idx):
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
                    or (
                        in_on_clause
                        and not expect_table
                        and (wu in _FROM_EXIT_KEYWORDS or wu in ("SELECT", "FROM", "JOIN", "TABLE"))
                        and _is_lex_on_expr_ident(processed, s_pos, sig_idxs)
                    )
                    or _is_lex_within_group(processed, s_pos, sig_idxs)
                ):
                    in_sample_clause = False
                    s_pos += 1
                    continue
                if (wu == "FROM" and not _is_lex_distinct_from(processed, s_pos, sig_idxs)) or wu == "JOIN":
                    if wu == "FROM" and top_from_s_pos is None:
                        top_from_s_pos = s_pos
                    in_from = True
                    expect_table = True
                    in_sample_clause = False
                    in_on_clause = False
                    s_pos += 1
                    continue
                if wu in _JOIN_MODIFIER_KEYWORDS:
                    if in_from and not (
                        in_on_clause
                        and s_pos + 1 < len(sig_idxs)
                        and (
                            processed[sig_idxs[s_pos + 1]] == ("OP", "(") or processed[sig_idxs[s_pos + 1]][0] == "SUBQ"
                        )
                    ):
                        expect_table = False
                        in_sample_clause = False
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
                    if top_from_s_pos is None:
                        top_from_s_pos = s_pos
                    in_from = False
                    expect_table = False
                    in_sample_clause = False
                    in_on_clause = False
                    s_pos += 1
                    continue
                if in_from and (wu == "ON" or (wu == "USING" and not in_on_clause)):
                    expect_table = False
                    in_sample_clause = False
                    in_on_clause = wu == "ON"
                    s_pos += 1
                    continue
                if in_from and not in_on_clause and wu in ("FINAL", "SAMPLE"):
                    expect_table = False
                    in_sample_clause = wu == "SAMPLE"
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
                    sub_doms = set(subquery_domains.get(t, set()))
                    sub_sch = subquery_schema.get(t, _DerivedCfSchema())
                    a_pos = s_pos + 1
                    if (
                        a_pos < len(sig_idxs)
                        and processed[sig_idxs[a_pos]][0] == "IDENT"
                        and processed[sig_idxs[a_pos]][1].upper() == "AS"
                    ):
                        a_pos += 1
                        if a_pos < len(sig_idxs) and processed[sig_idxs[a_pos]][0] in ("IDENT", "DQUOTE"):
                            col_list, end_sp = _consume_optional_col_alias_list(a_pos + 1)
                            if col_list is not None:
                                sub_doms = _demote_col_list_domains(sub_doms)
                                sub_sch = _remap_derived_cf_schema(sub_sch, col_list)
                            domains |= sub_doms
                            from_source_schemas.append(sub_sch)
                            branch_inherited_str_cf |= sub_sch.str_cols
                            branch_inherited_bool_cf |= sub_sch.bool_cols
                            branch_inherited_all_cols |= sub_sch.all_cols
                            _record_table_alias(sig_idxs[a_pos], sub_doms, sub_sch)
                            expect_table = False
                            s_pos = end_sp
                            continue
                    elif a_pos < len(sig_idxs) and (
                        processed[sig_idxs[a_pos]][0] == "DQUOTE"
                        or (
                            processed[sig_idxs[a_pos]][0] == "IDENT"
                            and (
                                processed[sig_idxs[a_pos]][1].upper() not in _SQL_CLAUSE_STOPWORDS
                                or _is_bare_clause_alias_at(a_pos)
                            )
                        )
                    ):
                        col_list, end_sp = _consume_optional_col_alias_list(a_pos + 1)
                        if col_list is not None:
                            sub_doms = _demote_col_list_domains(sub_doms)
                            sub_sch = _remap_derived_cf_schema(sub_sch, col_list)
                        domains |= sub_doms
                        from_source_schemas.append(sub_sch)
                        branch_inherited_str_cf |= sub_sch.str_cols
                        branch_inherited_bool_cf |= sub_sch.bool_cols
                        branch_inherited_all_cols |= sub_sch.all_cols
                        _record_table_alias(sig_idxs[a_pos], sub_doms, sub_sch)
                        expect_table = False
                        s_pos = end_sp
                        continue
                    domains |= sub_doms
                    from_source_schemas.append(sub_sch)
                    branch_inherited_str_cf |= sub_sch.str_cols
                    branch_inherited_bool_cf |= sub_sch.bool_cols
                    branch_inherited_all_cols |= sub_sch.all_cols
                    expect_table = False
                    s_pos += 1
                    continue
                if k in ("IDENT", "DQUOTE"):
                    if k == "IDENT" and s_pos + 1 < len(sig_idxs) and processed[sig_idxs[s_pos + 1]] == ("OP", "("):
                        from_source_schemas.append(_DerivedCfSchema())
                        expect_table = False
                        s_pos += 1
                        continue
                    low_tbl = _unquote_sql_ident(t).lower()
                    item_doms: set[str] = set()
                    item_sch = _DerivedCfSchema()
                    is_cte_ref = False
                    if low_tbl in ctes:
                        cte_doms, item_sch = ctes[low_tbl]
                        item_doms = set(cte_doms)
                        is_cte_ref = True
                    elif k == "IDENT" and low_tbl in table_map:
                        mapped_name, dom, canonical_name = table_map[low_tbl]
                        table_token_idxs[idx] = mapped_name
                        item_doms = {dom}
                        branch_canonicals.add(canonical_name)
                    elif k == "IDENT" and low_tbl == "as" and not _is_non_kw_lex_pos(processed, idx):
                        raise PermissionError("SQL parse error: expected table reference")
                    a_pos = s_pos + 1
                    if (
                        a_pos < len(sig_idxs)
                        and processed[sig_idxs[a_pos]][0] == "IDENT"
                        and processed[sig_idxs[a_pos]][1].upper() == "AS"
                    ):
                        a_pos += 1
                        if a_pos < len(sig_idxs) and processed[sig_idxs[a_pos]][0] in ("IDENT", "DQUOTE"):
                            col_list, end_sp = _consume_optional_col_alias_list(a_pos + 1)
                            if col_list is not None:
                                item_doms = _demote_col_list_domains(item_doms)
                                item_sch = _remap_derived_cf_schema(item_sch, col_list)
                                lp_wrap = sig_idxs[a_pos + 1]
                                rp_wrap = _find_matching_paren(processed, lp_wrap)
                                col_list_sql = "".join(tk for _, tk in processed[lp_wrap : rp_wrap + 1])
                                alias_raw = processed[sig_idxs[a_pos]][1]
                                rewritten_tbl = table_token_idxs.pop(idx, t)
                                ph_wrap = f"__fla_subq_{sub_nonce}_{len(subquery_placeholders)}__"
                                subquery_placeholders[ph_wrap] = (
                                    f"(SELECT * FROM {rewritten_tbl}) AS {alias_raw}{col_list_sql}"
                                )
                                col_list_table_spans[idx] = (rp_wrap, ph_wrap)
                            domains |= item_doms
                            from_source_schemas.append(item_sch)
                            branch_inherited_str_cf |= item_sch.str_cols
                            branch_inherited_bool_cf |= item_sch.bool_cols
                            branch_inherited_all_cols |= item_sch.all_cols
                            _record_table_alias(sig_idxs[a_pos], item_doms, item_sch)
                            expect_table = False
                            s_pos = end_sp
                            continue
                    elif a_pos < len(sig_idxs) and (
                        processed[sig_idxs[a_pos]][0] == "DQUOTE"
                        or (
                            processed[sig_idxs[a_pos]][0] == "IDENT"
                            and (
                                processed[sig_idxs[a_pos]][1].upper() not in _SQL_CLAUSE_STOPWORDS
                                or _is_bare_clause_alias_at(a_pos)
                            )
                        )
                    ):
                        col_list, end_sp = _consume_optional_col_alias_list(a_pos + 1)
                        if col_list is not None:
                            item_doms = _demote_col_list_domains(item_doms)
                            item_sch = _remap_derived_cf_schema(item_sch, col_list)
                            lp_wrap = sig_idxs[a_pos + 1]
                            rp_wrap = _find_matching_paren(processed, lp_wrap)
                            col_list_sql = "".join(tk for _, tk in processed[lp_wrap : rp_wrap + 1])
                            alias_raw = processed[sig_idxs[a_pos]][1]
                            rewritten_tbl = table_token_idxs.pop(idx, t)
                            ph_wrap = f"__fla_subq_{sub_nonce}_{len(subquery_placeholders)}__"
                            subquery_placeholders[ph_wrap] = (
                                f"(SELECT * FROM {rewritten_tbl}) AS {alias_raw}{col_list_sql}"
                            )
                            col_list_table_spans[idx] = (rp_wrap, ph_wrap)
                        domains |= item_doms
                        from_source_schemas.append(item_sch)
                        branch_inherited_str_cf |= item_sch.str_cols
                        branch_inherited_bool_cf |= item_sch.bool_cols
                        branch_inherited_all_cols |= item_sch.all_cols
                        _record_table_alias(sig_idxs[a_pos], item_doms, item_sch)
                        expect_table = False
                        s_pos = end_sp
                        continue
                    domains |= item_doms
                    if is_cte_ref:
                        alias_domains[low_tbl] = item_doms
                        alias_schema[low_tbl] = item_sch
                    elif idx in table_token_idxs:
                        alias_domains[low_tbl] = item_doms
                        alias_domains[table_token_idxs[idx].lower()] = item_doms
                    from_source_schemas.append(item_sch)
                    branch_inherited_str_cf |= item_sch.str_cols
                    branch_inherited_bool_cf |= item_sch.bool_cols
                    branch_inherited_all_cols |= item_sch.all_cols
                    expect_table = False
                    s_pos += 1
                    continue
                expect_table = False

            s_pos += 1

        raw_fact_domains = frozenset(_FACT_DOMAINS.values())
        touches_request = "request" in domains
        touches_any_fact = bool(domains & raw_fact_domains)

        # Analyze SELECT projection list for *-propagation, subquery AS <col> aliasing, and string/bool CF tracking:
        projects_star = False
        has_unknown_star_item = False
        auto_alias_after_idx: dict[int, str] = {}
        proj_alias_idxs: set[int] = set()
        out_str_cf_cols: set[str] = set()
        out_bool_cf_cols: set[str] = set()
        out_all_cols: set[str] = set()
        pos_flags: list[tuple[bool, bool]] = []
        _phys_req_star_cols = _REQUEST_TYPED_COLUMNS | {
            "timestamp",
            "event_timestamp",
            "ip",
            "client_ip",
            "custom_fields",
        }
        _non_bare_col_alias_words = (
            _SQL_CLAUSE_STOPWORDS
            | _ON_EXPR_PREV_WORDS
            | _ON_EXPR_NEXT_WORDS
            | _SQL_INTERVAL_UNITS
            | frozenset(w.upper() for w in _SQL_KEYWORDS)
            | {"END", "NULL", "TRUE", "FALSE", "ASC", "DESC", "FIRST", "LAST", "NULLS"}
        )
        _non_bare_col_expr_ends = (
            _SQL_CLAUSE_STOPWORDS
            | _ON_EXPR_PREV_WORDS
            | {"DISTINCT", "ALL", "CASE", "WHEN", "THEN", "ELSE", "INTERVAL", "CAST"}
        )
        if sig_idxs and processed[sig_idxs[0]][0] == "IDENT" and processed[sig_idxs[0]][1].upper() == "SELECT":
            sel_end = top_from_s_pos if top_from_s_pos is not None else len(sig_idxs)
            sel_start = 1
            if sel_start < sel_end and processed[sig_idxs[sel_start]][0] == "IDENT":
                mod_u = processed[sig_idxs[sel_start]][1].upper()
                if mod_u in ("DISTINCT", "ALL"):
                    sel_start += 1
                    if (
                        mod_u == "DISTINCT"
                        and sel_start + 1 < sel_end
                        and processed[sig_idxs[sel_start]][0] == "IDENT"
                        and processed[sig_idxs[sel_start]][1].upper() == "ON"
                        and processed[sig_idxs[sel_start + 1]] == ("OP", "(")
                    ):
                        on_close = _find_matching_paren(processed, sig_idxs[sel_start + 1])
                        while sel_start < sel_end and sig_idxs[sel_start] <= on_close:
                            sel_start += 1
            proj_items: list[list[int]] = []
            cur_item: list[int] = []
            p_depth = 0
            for sp in range(sel_start, sel_end):
                p_idx = sig_idxs[sp]
                pk, pt = processed[p_idx]
                if pk == "OP" and pt in ("(", "[", "{"):
                    p_depth += 1
                elif pk == "OP" and pt in (")", "]", "}"):
                    p_depth = max(0, p_depth - 1)
                elif p_depth == 0 and pk == "OP" and pt == ",":
                    if cur_item:
                        proj_items.append(cur_item)
                        cur_item = []
                    continue
                cur_item.append(p_idx)
            if cur_item:
                proj_items.append(cur_item)

            for it_idxs in proj_items:
                if len(it_idxs) == 1 and processed[it_idxs[0]] == ("OP", "*"):
                    projects_star = True
                    out_str_cf_cols |= branch_inherited_str_cf
                    out_bool_cf_cols |= branch_inherited_bool_cf
                    out_all_cols |= branch_inherited_all_cols
                    if touches_request:
                        out_all_cols |= _phys_req_star_cols
                    if len(from_source_schemas) == 1 and from_source_schemas[0].positional is not None:
                        pos_flags.extend(from_source_schemas[0].positional)
                    else:
                        has_unknown_star_item = True
                elif (
                    len(it_idxs) == 3
                    and processed[it_idxs[0]][0] in ("IDENT", "DQUOTE")
                    and processed[it_idxs[1]] == ("OP", ".")
                    and processed[it_idxs[2]] == ("OP", "*")
                ):
                    q_nm = _unquote_sql_ident(processed[it_idxs[0]][1]).lower()
                    q_dom = alias_domains.get(q_nm, domains)
                    if q_dom & raw_fact_domains:
                        projects_star = True
                    q_sch = alias_schema.get(q_nm, _DerivedCfSchema())
                    out_str_cf_cols |= q_sch.str_cols
                    out_bool_cf_cols |= q_sch.bool_cols
                    out_all_cols |= q_sch.all_cols
                    if "request" in q_dom:
                        out_all_cols |= _phys_req_star_cols
                    if q_sch.positional is not None:
                        pos_flags.extend(q_sch.positional)
                    else:
                        has_unknown_star_item = True
                elif len(it_idxs) == 1 and processed[it_idxs[0]][0] == "IDENT":
                    col_i = it_idxs[0]
                    col_raw = processed[col_i][1]
                    col_low = col_raw.lower()
                    out_all_cols.add(col_low)
                    is_s = False
                    is_b = False
                    if col_low == "custom_fields" and touches_request:
                        projects_star = True
                    elif (
                        (col_low == "timestamp" and touches_any_fact)
                        or (col_low == "ip" and touches_request)
                        or (col_low in _REQUEST_CF_FIELDS and touches_request)
                    ):
                        if is_subquery:
                            auto_alias_after_idx[col_i] = col_raw
                        if col_low in _REQUEST_CF_FIELDS and touches_request:
                            is_s = True
                            is_b = col_low in _REQUEST_BOOLEAN_CF_FIELDS
                    elif col_low in branch_inherited_str_cf:
                        is_s = True
                        is_b = col_low in branch_inherited_bool_cf
                    if is_s:
                        out_str_cf_cols.add(col_low)
                    if is_b:
                        out_bool_cf_cols.add(col_low)
                    pos_flags.append((is_s, is_b))
                elif (
                    len(it_idxs) == 3
                    and processed[it_idxs[0]][0] in ("IDENT", "DQUOTE")
                    and processed[it_idxs[1]] == ("OP", ".")
                    and processed[it_idxs[2]][0] == "IDENT"
                ):
                    q_nm = _unquote_sql_ident(processed[it_idxs[0]][1]).lower()
                    q_dom = alias_domains.get(q_nm, domains)
                    q_sch = alias_schema.get(
                        q_nm, _DerivedCfSchema(frozenset(branch_inherited_str_cf), frozenset(branch_inherited_bool_cf))
                    )
                    col_i = it_idxs[2]
                    col_raw = processed[col_i][1]
                    col_low = col_raw.lower()
                    out_all_cols.add(col_low)
                    is_s = False
                    is_b = False
                    if col_low == "custom_fields" and "request" in q_dom:
                        projects_star = True
                    elif (
                        (col_low == "timestamp" and bool(q_dom & raw_fact_domains))
                        or (col_low == "ip" and "request" in q_dom)
                        or (col_low in _REQUEST_CF_FIELDS and "request" in q_dom)
                    ):
                        if is_subquery:
                            auto_alias_after_idx[col_i] = col_raw
                        if col_low in _REQUEST_CF_FIELDS and "request" in q_dom:
                            is_s = True
                            is_b = col_low in _REQUEST_BOOLEAN_CF_FIELDS
                    elif col_low in q_sch.str_cols:
                        is_s = True
                        is_b = col_low in q_sch.bool_cols
                    if is_s:
                        out_str_cf_cols.add(col_low)
                    if is_b:
                        out_bool_cf_cols.add(col_low)
                    pos_flags.append((is_s, is_b))
                else:
                    explicit_as_alias = (
                        len(it_idxs) >= 3
                        and processed[it_idxs[-2]][0] == "IDENT"
                        and processed[it_idxs[-2]][1].upper() == "AS"
                        and processed[it_idxs[-1]][0] in ("IDENT", "DQUOTE")
                    )
                    bare_col_alias = (
                        not explicit_as_alias
                        and len(it_idxs) >= 2
                        and processed[it_idxs[-1]][0] in ("IDENT", "DQUOTE")
                        and (
                            processed[it_idxs[-1]][0] == "DQUOTE"
                            or processed[it_idxs[-1]][1].upper() not in _non_bare_col_alias_words
                        )
                        and (
                            processed[it_idxs[-2]][0] in ("DQUOTE", "STR", "NUM", "SUBQ")
                            or processed[it_idxs[-2]] in (("OP", ")"), ("OP", "]"))
                            or (
                                processed[it_idxs[-2]][0] == "IDENT"
                                and processed[it_idxs[-2]][1].upper() not in _non_bare_col_expr_ends
                            )
                        )
                    )
                    if explicit_as_alias or bare_col_alias:
                        proj_alias_idxs.add(it_idxs[-1])
                        alias_low = _unquote_sql_ident(processed[it_idxs[-1]][1]).lower()
                        out_all_cols.add(alias_low)
                        expr_i = it_idxs[:-2] if explicit_as_alias else it_idxs[:-1]
                        is_s = False
                        is_b = False
                        if len(expr_i) == 1 and processed[expr_i[0]][0] == "IDENT":
                            e_low = processed[expr_i[0]][1].lower()
                            if e_low in _REQUEST_CF_FIELDS and touches_request:
                                is_s = True
                                is_b = e_low in _REQUEST_BOOLEAN_CF_FIELDS
                            elif e_low in branch_inherited_str_cf:
                                is_s = True
                                is_b = e_low in branch_inherited_bool_cf
                        elif (
                            len(expr_i) == 3
                            and processed[expr_i[0]][0] in ("IDENT", "DQUOTE")
                            and processed[expr_i[1]] == ("OP", ".")
                            and processed[expr_i[2]][0] == "IDENT"
                        ):
                            q_nm = _unquote_sql_ident(processed[expr_i[0]][1]).lower()
                            q_dom = alias_domains.get(q_nm, domains)
                            q_sch = alias_schema.get(
                                q_nm,
                                _DerivedCfSchema(
                                    frozenset(branch_inherited_str_cf), frozenset(branch_inherited_bool_cf)
                                ),
                            )
                            e_low = processed[expr_i[2]][1].lower()
                            if e_low in _REQUEST_CF_FIELDS and "request" in q_dom:
                                is_s = True
                                is_b = e_low in _REQUEST_BOOLEAN_CF_FIELDS
                            elif e_low in q_sch.str_cols:
                                is_s = True
                                is_b = e_low in q_sch.bool_cols
                        elif (
                            len(expr_i) >= 4
                            and processed[expr_i[-4]][0] == "IDENT"
                            and processed[expr_i[-4]][1].lower() == "custom_fields"
                            and processed[expr_i[-3]] == ("OP", "[")
                            and processed[expr_i[-1]] == ("OP", "]")
                        ):
                            is_s = True
                            cf_tok = processed[expr_i[-2]]
                            if cf_tok[0] == "STR" and len(cf_tok[1]) >= 2:
                                is_b = cf_tok[1][1:-1].lower() in _REQUEST_BOOLEAN_CF_FIELDS
                        if is_s:
                            out_str_cf_cols.add(alias_low)
                        if is_b:
                            out_bool_cf_cols.add(alias_low)
                        pos_flags.append((is_s, is_b))
                    else:
                        pos_flags.append((False, False))

        rewritten_tokens: list[tuple[str, str]] = []
        sig_pos_by_idx = {idx: pos for pos, idx in enumerate(sig_idxs)}
        skip_until_idx = -1
        for idx, (k, t) in enumerate(processed):
            if idx <= skip_until_idx:
                continue
            if idx in col_list_table_spans:
                end_span_idx, ph_span = col_list_table_spans[idx]
                rewritten_tokens.append(("SUBQ", ph_span))
                skip_until_idx = end_span_idx
                continue
            if idx in table_token_idxs:
                rewritten_tokens.append(("IDENT", table_token_idxs[idx]))
                continue
            if idx in table_alias_idxs or idx in proj_alias_idxs or k != "IDENT":
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
                    alias_domains.get(_unquote_sql_ident(qual_tok[1]).lower(), domains)
                    if (qual_tok and qual_tok[0] in ("IDENT", "DQUOTE"))
                    else domains
                )
                low_c = t.lower()
                alias_suffix = f" AS {auto_alias_after_idx[idx]}" if idx in auto_alias_after_idx else ""
                if low_c == "timestamp" and bool(q_domains & raw_fact_domains):
                    rewritten_tokens.append(("IDENT", f"event_timestamp{alias_suffix}"))
                elif low_c == "ip" and "request" in q_domains:
                    rewritten_tokens.append(("IDENT", f"client_ip{alias_suffix}"))
                elif low_c in _REQUEST_CF_FIELDS and "request" in q_domains:
                    rewritten_tokens.append(("IDENT", f"custom_fields['{low_c}']{alias_suffix}"))
                else:
                    rewritten_tokens.append((k, t))
                continue
            low_ident = t.lower()
            alias_suffix = f" AS {auto_alias_after_idx[idx]}" if idx in auto_alias_after_idx else ""
            if low_ident == "timestamp" and touches_any_fact:
                rewritten_tokens.append(("IDENT", f"event_timestamp{alias_suffix}"))
            elif low_ident == "ip" and touches_request:
                rewritten_tokens.append(("IDENT", f"client_ip{alias_suffix}"))
            elif low_ident in _REQUEST_CF_FIELDS and touches_request:
                rewritten_tokens.append(("IDENT", f"custom_fields['{low_ident}']{alias_suffix}"))
            else:
                rewritten_tokens.append((k, t))

        non_cf_shadowed_str = frozenset(c for sch in from_source_schemas for c in (sch.all_cols - sch.str_cols))
        non_cf_shadowed_bool = frozenset(c for sch in from_source_schemas for c in (sch.all_cols - sch.bool_cols))
        has_derived_str_cf = bool(branch_inherited_str_cf)
        if touches_request or has_derived_str_cf:
            branch_sql = _coerce_request_predicates(
                rewritten_tokens,
                touches_request=touches_request,
                derived_str_cols=frozenset(branch_inherited_str_cf),
                derived_bool_cols=frozenset(branch_inherited_bool_cf),
                alias_schema_map=alias_schema,
                non_cf_shadowed_str_cols=non_cf_shadowed_str,
                non_cf_shadowed_bool_cols=non_cf_shadowed_bool,
                unqualified_derived=has_derived_str_cf,
            )
        else:
            branch_sql = "".join(_normalize_iso(tk) if kd == "STR" else tk for kd, tk in rewritten_tokens)
        for ph, sub_rendered in subquery_placeholders.items():
            branch_sql = branch_sql.replace(ph, sub_rendered)

        if projects_star:
            exposed_domains = set(domains)
        else:
            exposed_domains = set()
            if "request" in domains or "request_derived" in domains:
                exposed_domains.add("request_derived")
            if domains & {"rum_vitals", "rum_errors", "rum_derived"}:
                exposed_domains.add("rum_derived")
        out_pos = None if (has_unknown_star_item or not pos_flags) else tuple(pos_flags)
        return (
            branch_sql,
            exposed_domains,
            branch_canonicals,
            _DerivedCfSchema(
                frozenset(out_str_cf_cols),
                frozenset(out_bool_cf_cols),
                out_pos,
                frozenset(out_all_cols),
            ),
        )

    tokens = [(m.lastgroup or "OTHER", m.group(0)) for m in _CONSOLE_SQL_LEX_RE.finditer(sql)]
    rewritten, _, canonicals, _ = _rewrite_tokens(tokens, {}, is_subquery=False)
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
    _reject_internal_cf_identifiers(raw_sql)
    _reject_ambiguous_syntax(raw_sql, allowed_tables | frozenset(names.values()))
    try:
        pre_val = validate_user_sql(
            raw_sql,
            session_id=session_id,
            service_id=service.service_id,
            allowed_tables=allowed_tables,
        )
    except SQLValidationError as exc:
        raise PermissionError(exc.message) from exc
    _reject_unsafe_functions(raw_sql, parse_tree=pre_val.parse_tree)
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
    _reject_unsafe_functions(sql, parse_tree=post_val.parse_tree)
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
        exec_fn: Any = client.execute
        if "rewrite_cf" in inspect.signature(exec_fn).parameters:
            res = exec_fn(sql, settings=settings, rewrite_cf=False)
        else:
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
    _cf_col_re = re.compile(r"arrayElement\(custom_fields,\s*'([A-Za-z0-9_]+)'\)")

    def _norm_col(col_key: str) -> str:
        m = _cf_col_re.fullmatch(col_key)
        return m.group(1) if m else col_key

    data: list[dict[str, Any]] = []
    raw_cols = list(res[0].keys()) if res else []
    seen_cols = set(raw_cols)
    col_map = {rk: (_norm_col(rk) if _norm_col(rk) not in seen_cols or _norm_col(rk) == rk else rk) for rk in raw_cols}
    cols = [col_map[rk] for rk in raw_cols]
    for d in res:
        row_copy = {col_map.get(k, k): v for k, v in d.items()}
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
