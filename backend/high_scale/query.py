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
from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import parse_qs, unquote, urlparse

import orjson
import zstandard as zstd

from backend.core.catalog import _CANONICAL_SYSTEM_FIELD_SPECS
from backend.core.log_fields import LOG_FIELD_CATALOG, SqlType, try_get
from backend.core.share_db import (
    CMCD_SESSION_ALIAS_KEYS,
    PII_IP_KEYS,
    PII_SESSION_KEYS,
    apply_pii_policy,
    is_saved_view_pii_key,
    mask_ip_or_chain,
    mask_ip_values,
    scrub_ip_text,
)
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


def _ast_has_parse_error(node_obj: Any) -> bool:
    if isinstance(node_obj, dict):
        if node_obj.get("type") in ("PARSE_ERROR_NODE", "DISALLOWED_SUBQUERY_STATEMENT"):
            return True
        return any(_ast_has_parse_error(v_child) for v_child in node_obj.values())
    if isinstance(node_obj, list):
        return any(_ast_has_parse_error(item) for item in node_obj)
    return False


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
    known_relations: set[str] | None
    if eff_tree is not None:
        known_relations = set() if _ast_has_parse_error(eff_tree) else _collect_ast_base_tables_for_exempt(eff_tree)
    else:
        known_relations = None

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

    _delim_pairs_ex = {"(": ")", "[": "]", "{": "}"}
    _delim_close_ex = frozenset(_delim_pairs_ex.values())
    _delim_cache_ex: dict[int, tuple[list[tuple[str, str, int]], dict[int, int]]] = {}

    def _get_delim_map(s_list: list[tuple[str, str, int]], *, enforce_depth: bool = False) -> dict[int, int]:
        cached = _delim_cache_ex.get(id(s_list))
        if cached is not None and cached[0] is s_list:
            return cached[1]
        dmap: dict[int, int] = {}
        dstack: list[tuple[int, str]] = []
        for idx, (k, t, _) in enumerate(s_list):
            if k == "OP" and t in _delim_pairs_ex:
                dstack.append((idx, _delim_pairs_ex[t]))
                if enforce_depth and len(dstack) > _MAX_PAREN_DEPTH:
                    raise SQLValidationError("nesting_depth_exceeded", "SQL nesting depth limit exceeded")
            elif k == "OP" and t in _delim_close_ex:
                if dstack and dstack[-1][1] == t:
                    oi, _ = dstack.pop()
                    dmap[oi] = idx
                    dmap[idx] = oi
                else:
                    dstack.clear()
        _delim_cache_ex[id(s_list)] = (s_list, dmap)
        return dmap

    _get_delim_map(sig, enforce_depth=True)

    def _match_delim(s_list: list[tuple[str, str, int]], open_i: int, open_ch: str = "(", close_ch: str = ")") -> int:
        if open_i < 0 or open_i >= len(s_list) or (s_list[open_i][0], s_list[open_i][1]) != ("OP", open_ch):
            return -1
        dmap = _get_delim_map(s_list)
        ci = dmap.get(open_i, -1)
        if ci > open_i and (s_list[ci][0], s_list[ci][1]) == ("OP", close_ch):
            return ci
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

    def _peel_s_list_parens(s_list: list[tuple[str, str, int]]) -> list[tuple[str, str, int]]:
        if len(s_list) < 2 or (s_list[0][0], s_list[0][1]) != ("OP", "("):
            return s_list
        dmap = _get_delim_map(s_list)
        lo = 0
        hi = len(s_list) - 1
        while lo < hi and (s_list[lo][0], s_list[lo][1]) == ("OP", "(") and dmap.get(lo) == hi:
            lo += 1
            hi -= 1
        return s_list if (lo == 0 and hi == len(s_list) - 1) else s_list[lo : hi + 1]

    def _looks_like_subquery(sub_s: list[tuple[str, str, int]]) -> bool:
        cur = _peel_s_list_parens(sub_s)
        while len(cur) >= 2 and (cur[0][0], cur[0][1]) == ("OP", "("):
            close_first = _match_delim(cur, 0)
            if (
                0 < close_first < len(cur) - 1
                and cur[close_first + 1][0] == "IDENT"
                and cur[close_first + 1][1].upper()
                in ("UNION", "INTERSECT", "EXCEPT", "ORDER", "LIMIT", "OFFSET", "FETCH")
            ):
                cur = _peel_s_list_parens(cur[1:close_first])
                continue
            break
        return bool(cur) and cur[0][0] == "IDENT" and cur[0][1].upper() in ("SELECT", "WITH", "FROM", "TABLE")

    def _walk_expr_tokens(sub_s: list[tuple[str, str, int]], ctes_in_scope: frozenset[str]) -> None:
        dmap = _get_delim_map(sub_s)
        idx = 0
        while idx < len(sub_s):
            k_e, t_e, _ = sub_s[idx]
            if k_e == "OP" and t_e == "(":
                cl = dmap.get(idx, -1)
                if cl > idx:
                    if _is_fn_call_open(sub_s, idx):
                        _walk_expr_tokens(sub_s[idx + 1 : cl], ctes_in_scope)
                    else:
                        lo_p, hi_p = idx + 1, cl - 1
                        while (
                            lo_p < hi_p and (sub_s[lo_p][0], sub_s[lo_p][1]) == ("OP", "(") and dmap.get(lo_p) == hi_p
                        ):
                            lo_p += 1
                            hi_p -= 1
                        _walk_paren_group(sub_s[lo_p : hi_p + 1], ctes_in_scope)
                    idx = cl + 1
                    continue
            elif k_e == "OP" and t_e in ("[", "{"):
                cl_b = dmap.get(idx, -1)
                if cl_b > idx:
                    _walk_expr_tokens(sub_s[idx + 1 : cl_b], ctes_in_scope)
                    idx = cl_b + 1
                    continue
            idx += 1

    def _walk_paren_group(sub_s: list[tuple[str, str, int]], ctes_in_scope: frozenset[str]) -> None:
        peeled = _peel_s_list_parens(sub_s)
        if _looks_like_subquery(peeled):
            _walk_query(peeled, ctes_in_scope)
            return
        _walk_expr_tokens(peeled, ctes_in_scope)

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
        s_list = _peel_s_list_parens(s_list)
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
        disallowed_unquoted_as = (
            (_FROM_EXIT_KEYWORDS - {"SAMPLE", "UNION"})
            | _JOIN_MODIFIER_KEYWORDS
            | frozenset(
                {
                    "JOIN",
                    "FROM",
                    "ON",
                    "USING",
                    "VALUES",
                    "SET",
                    "WITH",
                    "TABLE",
                    "AS",
                    "FINAL",
                    "LATERAL",
                    "ONLY",
                    "PIVOT",
                    "UNPIVOT",
                    "PIVOT_WIDER",
                    "PIVOT_LONGER",
                    "TABLESAMPLE",
                }
            )
        )
        disallowed_bare_alias = disallowed_unquoted_as | frozenset({"SELECT", "UNION", "SAMPLE"})
        in_from = False
        in_array_join = False
        expect_table = False
        in_on = False
        in_sample = False
        allow_post_table_alias = False
        saw_final = False
        in_join_mod = False
        saw_join = False

        jm_cache_start = -1
        jm_cache_end = -1
        jm_cache_ok = False

        def _starts_join_mod_seq(idx: int) -> bool:
            nonlocal jm_cache_start, jm_cache_end, jm_cache_ok
            if jm_cache_start <= idx < jm_cache_end:
                return jm_cache_ok
            q_i = idx
            while q_i < n and s_list[q_i][0] == "IDENT" and s_list[q_i][1].upper() in _JOIN_MODIFIER_KEYWORDS:
                q_i += 1
            ok = False
            if q_i < n and s_list[q_i][0] == "IDENT":
                qu = s_list[q_i][1].upper()
                if qu == "JOIN":
                    ok = True
                elif (
                    qu == "ARRAY"
                    and q_i + 1 < n
                    and s_list[q_i + 1][0] == "IDENT"
                    and s_list[q_i + 1][1].upper() == "JOIN"
                ):
                    ok = True
            jm_cache_start = idx
            jm_cache_end = q_i
            jm_cache_ok = ok
            return ok

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
                    allow_post_table_alias = end_alias is None
                    saw_final = False
                    in_join_mod = False
                    pos = end_alias if end_alias is not None else cl + 1
                    continue
                if is_fn_call:
                    _walk_expr_tokens(inner, active_ctes)
                else:
                    _walk_paren_group(inner, active_ctes)
                if in_from and expect_table:
                    expect_table = False
                    allow_post_table_alias = True
                    saw_final = False
                    in_join_mod = False
                elif in_from and not expect_table and not in_on and not in_sample and not in_array_join:
                    in_from = False
                    allow_post_table_alias = False
                    in_join_mod = False
                pos = cl + 1
                continue
            if k == "OP" and t in ("[", "{"):
                cl_br = _match_delim(s_list, pos, t, "]" if t == "[" else "}")
                if cl_br <= pos:
                    break
                _walk_expr_tokens(s_list[pos + 1 : cl_br], active_ctes)
                if in_from and (expect_table or (not in_on and not in_sample and not in_array_join)):
                    in_from = False
                    expect_table = False
                    allow_post_table_alias = False
                    in_join_mod = False
                pos = cl_br + 1
                continue
            prev_is_dot = pos >= 1 and (s_list[pos - 1][0], s_list[pos - 1][1]) == ("OP", ".")
            if (
                in_from
                and not expect_table
                and not in_on
                and not in_sample
                and not in_array_join
                and k == "IDENT"
                and t.upper() == "AS"
                and not prev_is_dot
            ):
                if (
                    not allow_post_table_alias
                    or in_join_mod
                    or pos + 1 >= n
                    or s_list[pos + 1][0] not in ("IDENT", "DQUOTE")
                ):
                    in_from = False
                    allow_post_table_alias = False
                    in_join_mod = False
                    pos += 1
                    continue
                ak, at, _ = s_list[pos + 1]
                if ak == "IDENT":
                    au = at.upper()
                    if au in disallowed_unquoted_as:
                        in_from = False
                        allow_post_table_alias = False
                        in_join_mod = False
                        pos += 2
                        continue
                    if au in ("SELECT", "UNION"):
                        nxt2 = s_list[pos + 2] if pos + 2 < n else None
                        nxt2_dot = pos + 3 < n and (s_list[pos + 3][0], s_list[pos + 3][1]) == ("OP", ".")
                        if not (
                            nxt2 is not None
                            and nxt2[0] == "IDENT"
                            and nxt2[1].upper() in ("WHERE", "ON")
                            and not nxt2_dot
                        ):
                            in_from = False
                            allow_post_table_alias = False
                            in_join_mod = False
                            pos += 2
                            continue
                end_as_col = _consume_alias_col_list(s_list, pos, record_ast_fn=True)
                allow_post_table_alias = False
                pos = end_as_col if end_as_col is not None else pos + 2
                continue
            prev_is_as_kw = (
                (not in_from or in_on or in_array_join)
                and not expect_table
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
                if (
                    in_from
                    and not expect_table
                    and not in_sample
                    and wu == "ARRAY"
                    and pos + 1 < n
                    and s_list[pos + 1][0] == "IDENT"
                    and s_list[pos + 1][1].upper() == "JOIN"
                ):
                    in_array_join = True
                    expect_table = False
                    in_on = False
                    allow_post_table_alias = False
                    in_join_mod = False
                    pos += 2
                    continue
                if in_from and wu == "JOIN" and is_prev_array:
                    in_array_join = True
                    expect_table = False
                    in_on = False
                    allow_post_table_alias = False
                    in_join_mod = False
                    pos += 1
                    continue
                if (wu == "FROM" and not is_distinct_from) or (in_from and not expect_table and wu == "JOIN"):
                    in_from = True
                    if wu == "FROM":
                        in_array_join = False
                        saw_join = False
                    else:
                        in_array_join = False
                        saw_join = True
                    expect_table = True
                    in_on = False
                    in_sample = False
                    allow_post_table_alias = False
                    saw_final = False
                    in_join_mod = False
                    pos += 1
                    continue
                if in_from and not expect_table and wu in _JOIN_MODIFIER_KEYWORDS:
                    if _starts_join_mod_seq(pos):
                        expect_table = False
                        in_on = False
                        in_sample = False
                        in_array_join = False
                        allow_post_table_alias = False
                        in_join_mod = True
                        pos += 1
                        continue
                    if not in_on and not in_sample and not in_array_join:
                        in_from = False
                        allow_post_table_alias = False
                        in_join_mod = False
                        pos += 1
                        continue
                if in_from and expect_table and wu in ("LATERAL", "ONLY"):
                    pos += 1
                    continue
                if (
                    in_from
                    and not expect_table
                    and not in_on
                    and not in_sample
                    and not in_array_join
                    and not in_join_mod
                    and not saw_final
                    and wu == "FINAL"
                ):
                    saw_final = True
                    allow_post_table_alias = False
                    pos += 1
                    continue
                if (
                    in_from
                    and not expect_table
                    and not in_on
                    and not in_array_join
                    and not in_join_mod
                    and wu == "TABLESAMPLE"
                ):
                    in_sample = True
                    allow_post_table_alias = False
                    pos += 1
                    continue
                if (
                    in_from
                    and not expect_table
                    and not in_on
                    and not in_sample
                    and not in_array_join
                    and not in_join_mod
                    and wu in ("SETTINGS", "FORMAT", "PREWHERE", "SAMPLE", "FOR", "SET")
                ):
                    nxt_s = s_list[pos + 1] if pos + 1 < n else None
                    nxt_s_dot = pos + 2 < n and (s_list[pos + 2][0], s_list[pos + 2][1]) == ("OP", ".")
                    is_lparen_ctx_alias = False
                    if nxt_s is not None and (nxt_s[0], nxt_s[1]) == ("OP", "("):
                        if wu == "PREWHERE":
                            rp_pw = _match_delim(s_list, pos + 1)
                            after_pw = s_list[rp_pw + 1] if (rp_pw > pos + 1 and rp_pw + 1 < n) else None
                            after_pw_dot = (
                                rp_pw > pos + 1
                                and rp_pw + 2 < n
                                and (s_list[rp_pw + 2][0], s_list[rp_pw + 2][1]) == ("OP", ".")
                            )
                            is_lparen_ctx_alias = (
                                rp_pw > pos + 1
                                and _valid_ident_list(s_list[pos + 2 : rp_pw])
                                and after_pw is not None
                                and (
                                    (after_pw[0], after_pw[1]) == ("OP", ",")
                                    or (
                                        after_pw[0] == "IDENT"
                                        and not after_pw_dot
                                        and after_pw[1].upper() in (_JOIN_MODIFIER_KEYWORDS | {"JOIN", "FINAL"})
                                    )
                                )
                            )
                        else:
                            is_lparen_ctx_alias = True
                    is_bare_ctx_alias = (
                        allow_post_table_alias
                        and not saw_final
                        and (
                            nxt_s is None
                            or (nxt_s[0], nxt_s[1]) == ("OP", ",")
                            or is_lparen_ctx_alias
                            or (
                                nxt_s[0] == "IDENT"
                                and not nxt_s_dot
                                and nxt_s[1].upper()
                                in (
                                    _JOIN_MODIFIER_KEYWORDS
                                    | _FROM_EXIT_KEYWORDS
                                    | {"JOIN", "ON", "USING", "FINAL", "SELECT"}
                                )
                            )
                        )
                    )
                    if is_bare_ctx_alias or wu == "SAMPLE":
                        in_sample = wu == "SAMPLE" and not is_bare_ctx_alias
                        allow_post_table_alias = False
                        if (
                            is_bare_ctx_alias
                            and pos + 1 < n
                            and (s_list[pos + 1][0], s_list[pos + 1][1]) == ("OP", "(")
                        ):
                            end_ctx = _consume_alias_col_list(s_list, pos, record_ast_fn=True)
                            pos = end_ctx if end_ctx is not None else pos + 1
                        else:
                            pos += 1
                        continue
                if in_from and in_sample and wu == "OFFSET":
                    pos += 1
                    continue
                if in_from and not expect_table and (wu == "ON" or (wu == "USING" and not in_on)):
                    if (
                        wu == "USING"
                        and not in_join_mod
                        and pos + 1 < n
                        and s_list[pos + 1][0] == "IDENT"
                        and s_list[pos + 1][1].upper() == "SAMPLE"
                    ):
                        in_sample = True
                        allow_post_table_alias = False
                        pos += 2
                        continue
                    if in_join_mod or not saw_join:
                        in_from = False
                        in_on = False
                        in_sample = False
                        allow_post_table_alias = False
                        in_join_mod = False
                        saw_join = False
                        pos += 1
                        continue
                    expect_table = False
                    in_sample = False
                    allow_post_table_alias = False
                    if wu == "ON":
                        in_on = True
                        pos += 1
                        continue
                    if pos + 1 < n and (s_list[pos + 1][0], s_list[pos + 1][1]) == ("OP", "("):
                        rp_u = _match_delim(s_list, pos + 1)
                        if rp_u > pos + 1 and _valid_ident_list(s_list[pos + 2 : rp_u]):
                            in_on = False
                            pos = rp_u + 1
                            continue
                    in_from = False
                    in_on = False
                    pos += 1
                    continue
                if (
                    wu == "SELECT"
                    and in_from
                    and not expect_table
                    and not in_on
                    and not in_sample
                    and not in_array_join
                    and allow_post_table_alias
                    and not in_join_mod
                    and not saw_final
                ):
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
                        allow_post_table_alias = False
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
                    in_sample = False
                    allow_post_table_alias = False
                    saw_final = False
                    in_join_mod = False
                    saw_join = False
                    pos += 1
                    continue
            if in_from and not expect_table and (k, t) == ("OP", ","):
                if in_join_mod:
                    in_from = False
                    in_join_mod = False
                    pos += 1
                    continue
                in_on = False
                in_sample = False
                allow_post_table_alias = False
                saw_final = False
                saw_join = False
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
                                allow_post_table_alias = end_tf_alias is None
                                pos = end_tf_alias if end_tf_alias is not None else cl_fn + 1
                            else:
                                in_from = False
                                allow_post_table_alias = False
                                pos = cl_fn + 1
                        else:
                            in_from = False
                            allow_post_table_alias = False
                            pos = j + 1
                        expect_table = False
                        saw_final = False
                        in_join_mod = False
                        continue
                    if (
                        len(parts) == 1
                        and parts[0][0] == "IDENT"
                        and parts[0][1].upper() in non_table_or_alias_keywords
                    ):
                        in_from = False
                        expect_table = False
                        allow_post_table_alias = False
                        in_join_mod = False
                        pos = j
                        continue
                    tbl_low = _unquote_tok(parts[-1][0], parts[-1][1]).lower()
                    if known_relations is not None and tbl_low not in (known_relations | active_ctes):
                        in_from = False
                        expect_table = False
                        allow_post_table_alias = False
                        in_join_mod = False
                        pos = j
                        continue
                    end_alias = _consume_alias_col_list(s_list, j, record_ast_fn=True)
                    expect_table = False
                    allow_post_table_alias = end_alias is None
                    saw_final = False
                    in_join_mod = False
                    pos = end_alias if end_alias is not None else j
                    continue
                in_from = False
                expect_table = False
                allow_post_table_alias = False
                in_join_mod = False
                pos += 1
                continue
            if in_from and not expect_table and not in_on and not in_sample and not in_array_join:
                if (
                    allow_post_table_alias
                    and not in_join_mod
                    and not saw_final
                    and k in ("IDENT", "DQUOTE")
                    and not (k == "IDENT" and t.upper() in disallowed_bare_alias)
                ):
                    allow_post_table_alias = False
                    pos += 1
                    continue
                in_from = False
                allow_post_table_alias = False
                in_join_mod = False
            pos += 1

    _walk_query(sig)
    return exempt


_IN_PREFIX_PREV_KEYWORDS = frozenset(
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
        "COLLATE",
        "BY",
        "LIMIT",
        "OFFSET",
        "FETCH",
        "FIRST",
        "NEXT",
        "STEP",
        "STALENESS",
        "RETURN",
        "SET",
        "VALUES",
        "PARTITION",
        "USING",
        "OVER",
        "FILTER",
        "AS",
        "FROM",
        "JOIN",
        "UNION",
        "INTERSECT",
        "EXCEPT",
        "WITH",
        "WINDOW",
        "ARRAY",
        "SAMPLE",
        "SETTINGS",
    }
)


def _find_open_paren_sig(
    sig: list[tuple[str, str, int]], close_idx: int, paren_map: dict[int, int] | None = None
) -> int:
    if paren_map is not None:
        lp = paren_map.get(close_idx, -1)
        return lp if 0 <= lp < close_idx else -1
    depth = 0
    for j in range(close_idx, -1, -1):
        jk, jt, _ = sig[j]
        if jk == "OP":
            if jt == ")":
                depth += 1
            elif jt == "(":
                depth -= 1
                if depth == 0:
                    return j
    return -1


def _is_select_head_top_prefix_sig(
    sig: list[tuple[str, str, int]], top_idx: int, paren_map: dict[int, int] | None = None
) -> bool:
    if top_idx < 1 or sig[top_idx][0] != "IDENT" or sig[top_idx][1].upper() != "TOP":
        return False
    if sig[top_idx - 1][0] == "IDENT" and sig[top_idx - 1][1].upper() == "SELECT":
        return True
    if (
        top_idx >= 2
        and sig[top_idx - 1][0] == "IDENT"
        and sig[top_idx - 1][1].upper() in ("ALL", "DISTINCT")
        and sig[top_idx - 2][0] == "IDENT"
        and sig[top_idx - 2][1].upper() == "SELECT"
    ):
        return True
    if (sig[top_idx - 1][0], sig[top_idx - 1][1]) == ("OP", ")"):
        lp = _find_open_paren_sig(sig, top_idx - 1, paren_map)
        return (
            lp >= 3
            and sig[lp - 1][0] == "IDENT"
            and sig[lp - 1][1].upper() == "ON"
            and sig[lp - 2][0] == "IDENT"
            and sig[lp - 2][1].upper() == "DISTINCT"
            and sig[lp - 3][0] == "IDENT"
            and sig[lp - 3][1].upper() == "SELECT"
        )
    return False


def _is_select_head_top_end_sig(
    sig: list[tuple[str, str, int]], pos: int, paren_map: dict[int, int] | None = None
) -> bool:
    if pos < 1 or pos >= len(sig):
        return False
    pk, pt, _ = sig[pos]
    if pk == "NUM":
        tp = pos - 1
        if tp >= 1 and sig[tp][0] == "OP" and sig[tp][1] in ("+", "-"):
            tp -= 1
        return _is_select_head_top_prefix_sig(sig, tp, paren_map)
    if (pk, pt) == ("OP", ")"):
        lp = _find_open_paren_sig(sig, pos, paren_map)
        return lp >= 2 and _is_select_head_top_prefix_sig(sig, lp - 1, paren_map)
    return False


def _is_select_head_modifier_end_sig(
    sig: list[tuple[str, str, int]], pos: int, paren_map: dict[int, int] | None = None
) -> bool:
    if pos < 1 or pos >= len(sig):
        return False
    if _is_select_head_top_end_sig(sig, pos, paren_map):
        return True
    pk, pt, _ = sig[pos]
    if (pk, pt) == ("OP", ")"):
        lp = _find_open_paren_sig(sig, pos, paren_map)
        return (
            lp >= 3
            and sig[lp - 1][0] == "IDENT"
            and sig[lp - 1][1].upper() == "ON"
            and sig[lp - 2][0] == "IDENT"
            and sig[lp - 2][1].upper() == "DISTINCT"
            and sig[lp - 3][0] == "IDENT"
            and sig[lp - 3][1].upper() == "SELECT"
        )
    if (
        pos >= 3
        and pk == "IDENT"
        and pt.upper() == "TIES"
        and sig[pos - 1][0] == "IDENT"
        and sig[pos - 1][1].upper() == "WITH"
    ):
        return _is_select_head_top_end_sig(sig, pos - 2, paren_map)
    return False


def _is_prefix_in_call_sig(
    sig: list[tuple[str, str, int]], idx: int, *, paren_map: dict[int, int] | None = None
) -> bool:
    """Return True when ``in(...)`` at ``idx`` is a prefix function call (no LHS operand)."""
    p = idx - 1
    if p >= 0 and sig[p][0] == "IDENT" and sig[p][1].upper() == "NOT":
        p -= 1
    if p >= 0 and sig[p][0] == "IDENT" and sig[p][1].upper() == "GLOBAL":
        p -= 1
    if p < 0:
        return True
    if _is_select_head_modifier_end_sig(sig, p, paren_map):
        return True
    pk, pt, _ = sig[p]
    if pk == "OP":
        return pt not in (")", "]", "}")
    if pk == "OTHER":
        return True
    if pk == "NUM":
        tp = p - 1
        if tp >= 1 and sig[tp][0] == "OP" and sig[tp][1] in ("+", "-"):
            tp -= 1
        return tp >= 0 and sig[tp][0] == "IDENT" and sig[tp][1].upper() == "TOP"
    if pk == "IDENT":
        if p > 0 and (sig[p - 1][0], sig[p - 1][1]) == ("OP", "."):
            return False
        return pt.upper() in _IN_PREFIX_PREV_KEYWORDS
    return False


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
    sig_paren_map: dict[int, int] = {}
    if "(" in sql:
        _pstack: list[int] = []
        for s_i, (sk, st, _) in enumerate(sig):
            if sk == "OP":
                if st == "(":
                    _pstack.append(s_i)
                elif st == ")":
                    if _pstack:
                        op_i = _pstack.pop()
                        sig_paren_map[op_i] = s_i
                        sig_paren_map[s_i] = op_i
                    else:
                        _pstack.clear()
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
        if lowered == "in":
            if _is_prefix_in_call_sig(sig, idx, paren_map=sig_paren_map):
                raise PermissionError(f"function {tok!r} is not allowed in console queries")
            continue
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
                    elif (fl == "in" or fl not in _SQL_KEYWORDS) and fl not in _ALLOWED_FUNCTIONS:
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

_FAST_CON_LEX_SPECIAL_RE = re.compile(r"['\"`]|--|/\*|\.[0-9]|[0-9][.eExXbBoO_]")
_FAST_CON_LEX_RE = re.compile(
    r"(?P<IDENT>" + SAFE_IDENT_RE.pattern + ")"
    r"|(?P<WS>\s+)"
    r"|(?P<OP>[.,()\[\]{}*+-/;]|<=|>=|!=|<>|[=<>])"
    r"|(?P<NUM>[0-9]+)"
    r"|(?P<OTHER>.)"
)
_CONSOLE_SQL_LEX_RE = re.compile(
    r"(?P<IDENT>" + SAFE_IDENT_RE.pattern + ")"
    r"|(?P<WS>\s+)"
    r"|(?P<COMMENT>--[^\n]*|/\*[\s\S]*?\*/)"
    r"|(?P<STR>'(?:''|[^'])*')"
    r'|(?P<DQUOTE>"(?:""|[^"])*"|`(?:``|[^`])*`)'
    r'|(?P<NUM>(?:0(?:[xX][0-9a-fA-F][0-9a-fA-F_]*(?:\.(?!\.)[0-9a-fA-F_]*)?(?:[pP][+-]?\d+(?:_\d+)*)?|[bB][01][01_]*|[oO][0-7][0-7_]*)|(?:\d+(?:_\d+)*(?:\.(?!\.)(?:\d+(?:_\d+)*)?)?|(?<![A-Za-z0-9_)\]}"\'`])\.\d+(?:_\d+)*)(?:[eE][+-]?\d+(?:_\d+)*)?))'
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
_MAX_PAREN_DEPTH = 64
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
        "POSITIONAL",
        "PASTE",
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
    if "_cf_" not in sql.lower():
        return
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

    _lex_delim_pairs = {"(": ")", "[": "]", "{": "}"}
    _lex_delim_close = frozenset(_lex_delim_pairs.values())
    _lex_meta_cache: dict[
        int,
        tuple[
            list[tuple[str, str]],
            list[int],
            dict[int, int],
            list[int],
            dict[int, int],
            dict[int, int],
            dict[int, bool],
            dict[str, Any],
        ],
    ] = {}

    _has_any_delim_sql = any(ch in sql for ch in "()[]{}")
    _sql_upper_scan = sql.upper()
    _has_any_as_sql = "AS" in _sql_upper_scan
    _has_any_end_sql = "END" in _sql_upper_scan
    _has_any_dot_sql = "." in sql
    _has_any_non_kw_sql = _has_any_as_sql or _has_any_dot_sql or ":" in sql
    _has_any_quote_or_comment_sql = "'" in sql or '"' in sql or "`" in sql or "--" in sql or "/*" in sql
    _has_select_head_mod_sql = "TOP" in _sql_upper_scan or "DISTINCT" in _sql_upper_scan

    def _get_lex_meta_entry(
        tokens: list[tuple[str, str]], *, enforce_depth: bool = False
    ) -> tuple[list[int], dict[int, int], list[int], dict[int, int], dict[int, int], dict[int, bool], dict[str, Any]]:
        cached = _lex_meta_cache.get(id(tokens))
        if cached is not None and cached[0] is tokens:
            return cached[1], cached[2], cached[3], cached[4], cached[5], cached[6], cached[7]
        match_by_tok: dict[int, int] = {}
        match_by_s: dict[int, int] = {}
        if not _has_any_delim_sql and not _has_any_as_sql:
            sig_list = [tok_idx for tok_idx, (k, _) in enumerate(tokens) if k != "WS" and k != "COMMENT"]
            sig_pos_map = dict(zip(sig_list, range(len(sig_list))))
            as_run = [0] * len(sig_list)
        else:
            sig_list = []
            sig_pos_map = {}
            sig_append = sig_list.append
            as_run = []
            as_append = as_run.append
            stack: list[tuple[int, int, str]] = []
            run = 0
            check_delim = _has_any_delim_sql
            check_as = _has_any_as_sql
            for tok_idx, (k, t) in enumerate(tokens):
                if k == "WS" or k == "COMMENT":
                    continue
                s_pos = len(sig_list)
                sig_append(tok_idx)
                sig_pos_map[tok_idx] = s_pos
                if check_as:
                    as_append(run)
                    if k == "IDENT" and len(t) == 2 and t.upper() == "AS":
                        run += 1
                    else:
                        run = 0
                if check_delim and k == "OP":
                    if t in _lex_delim_pairs:
                        stack.append((tok_idx, s_pos, _lex_delim_pairs[t]))
                        if enforce_depth and len(stack) > _MAX_PAREN_DEPTH:
                            raise SQLValidationError("nesting_depth_exceeded", "SQL nesting depth limit exceeded")
                    elif t in _lex_delim_close:
                        if stack and stack[-1][2] == t:
                            ot, os_p, _ = stack.pop()
                            match_by_tok[ot] = tok_idx
                            match_by_tok[tok_idx] = ot
                            match_by_s[os_p] = s_pos
                            match_by_s[s_pos] = os_p
                        else:
                            stack.clear()
            if not check_as:
                as_run = [0] * len(sig_list)
        non_kw_memo: dict[int, bool] = {}
        case_state: dict[str, Any] = {
            "ready": False,
            "building": False,
            "map": {},
            "on_op": {},
            "fact": {},
            "clause_open": {},
        }
        _lex_meta_cache[id(tokens)] = (
            tokens,
            sig_list,
            sig_pos_map,
            as_run,
            match_by_tok,
            match_by_s,
            non_kw_memo,
            case_state,
        )
        return sig_list, sig_pos_map, as_run, match_by_tok, match_by_s, non_kw_memo, case_state

    def _get_lex_meta(
        tokens: list[tuple[str, str]], *, enforce_depth: bool = False
    ) -> tuple[list[int], dict[int, int], list[int], dict[int, int], dict[int, int], dict[int, bool]]:
        sig_list, sig_pos_map, as_run, match_by_tok, match_by_s, non_kw_memo, _ = _get_lex_meta_entry(
            tokens, enforce_depth=enforce_depth
        )
        return sig_list, sig_pos_map, as_run, match_by_tok, match_by_s, non_kw_memo

    def _sig_indices(tokens: list[tuple[str, str]]) -> list[int]:
        return _get_lex_meta(tokens)[0]

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
        del sig_list
        _, _, _, _, match_by_s, _ = _get_lex_meta(tokens)
        open_s = match_by_s.get(close_s_pos, -1)
        return open_s if 0 <= open_s < close_s_pos else -1

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
        if not _has_select_head_mod_sql or s_pos < 1 or s_pos >= len(sig_list):
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
        if s_pos <= 0 or s_pos >= len(sig_list) or tokens[sig_list[s_pos]] != ("OTHER", "!"):
            return False
        return _eval_lex_on_op_and_fact(tokens, "fact", s_pos, sig_list)

    def _build_lex_case_when_end_map(tokens: list[tuple[str, str]], sig_list: list[int]) -> dict[int, bool]:
        _, _, _, _, match_by_s, _ = _get_lex_meta(tokens)
        case_map: dict[int, bool] = {}
        frames: list[list[list[Any]]] = [[]]
        for k_s, tok_idx in enumerate(sig_list):
            tk, tt = tokens[tok_idx]
            if tk == "OP" and tt in ("(", "[", "{"):
                if match_by_s.get(k_s, -1) <= k_s:
                    frames[-1].clear()
                frames.append([])
                continue
            if tk == "OP" and tt in (")", "]", "}"):
                if len(frames) > 1:
                    frames.pop()
                else:
                    frames[0].clear()
                continue
            if tk == "OP" and tt == ";":
                frames[-1].clear()
                continue
            if tk == "IDENT":
                wu = tt.upper()
                if (
                    wu in ("CASE", "WHEN", "END")
                    and not _is_non_kw_lex_pos(tokens, tok_idx)
                    and not _is_lex_dot_qualifier(tokens, k_s, sig_list)
                ):
                    if wu == "CASE":
                        frames[-1].append([k_s, False])
                    elif wu == "WHEN":
                        if frames[-1]:
                            frames[-1][-1][1] = True
                    elif wu == "END":
                        if frames[-1]:
                            c_s, saw_when = frames[-1].pop()
                            if saw_when:
                                case_map[c_s] = True
        return case_map

    def _has_matching_case_when_end_lex(tokens: list[tuple[str, str]], case_s_pos: int, sig_list: list[int]) -> bool:
        if not _has_any_end_sql:
            return False
        _, _, _, _, _, _, case_state = _get_lex_meta_entry(tokens)
        if case_state["ready"]:
            return bool(case_state["map"].get(case_s_pos, False))
        if case_state["building"]:
            return False
        case_state["building"] = True
        try:
            case_state["map"] = _build_lex_case_when_end_map(tokens, sig_list)
            case_state["ready"] = True
        finally:
            case_state["building"] = False
        return bool(case_state["map"].get(case_s_pos, False))

    def _cannot_end_aliasable_lex(tokens: list[tuple[str, str]], pos: int | None, r: int, after_as_idx: int) -> bool:
        if pos is None or pos < 0 or _is_dot_or_dcolon_lex(tokens, pos):
            return True
        pk, pt = tokens[pos]
        if pk == "OP" and pt not in (")", "]", "}"):
            return True
        sig_list, sig_pos_map, _, _, _, _ = _get_lex_meta(tokens)
        s_pos = sig_pos_map.get(pos, -1)
        if pk == "OTHER":
            if pt != "!":
                return True
            if (r % 2) == 0:
                return False
            if s_pos < 0:
                return True
            return not _is_postfix_factorial_lex(tokens, s_pos, sig_list)
        if pk in ("NUM", "SUBQ") or (pk == "OP" and pt == ")") or (pk == "IDENT" and pt.upper() == "TIES"):
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
                        after_s = sig_pos_map.get(after_as_idx, -1)
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
        if s_pos < 0:
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
            return _is_clause_open_word_lex(tokens, s_pos, sig_list)
        return False

    def _is_clause_open_word_lex(tokens: list[tuple[str, str]], s_pos: int, sig_list: list[int]) -> bool:
        _, _, _, _, _, _, case_state = _get_lex_meta_entry(tokens)
        co_memo: dict[int, bool] = case_state["clause_open"]
        if s_pos in co_memo:
            return co_memo[s_pos]
        cur_s = s_pos
        while cur_s > 0:
            if (cur_s - 1) in co_memo:
                break
            prev_i = sig_list[cur_s - 1]
            prev_k, prev_t = tokens[prev_i]
            if (
                prev_k == "IDENT"
                and prev_t.upper() in _AS_CLAUSE_OPENING_WORDS
                and not _is_non_kw_lex_pos(tokens, prev_i)
                and not (prev_t.upper() == "FROM" and _is_lex_distinct_from(tokens, cur_s - 1, sig_list))
            ):
                cur_s -= 1
                continue
            break
        if cur_s > 0 and (cur_s - 1) in co_memo:
            cur_val = not co_memo[cur_s - 1]
        elif cur_s == 0:
            cur_val = True
        else:
            prev_idx = sig_list[cur_s - 1]
            prev_k, prev_t = tokens[prev_idx]
            cur_val = True
            is_star_wildcard = (prev_k, prev_t) == ("OP", "*") and (
                cur_s == 1
                or tokens[sig_list[cur_s - 2]] == ("OP", ".")
                or tokens[sig_list[cur_s - 2]] == ("OP", ",")
                or (
                    tokens[sig_list[cur_s - 2]][0] == "IDENT"
                    and tokens[sig_list[cur_s - 2]][1].upper() in ("SELECT", "DISTINCT", "ALL")
                    and not _is_non_kw_lex_pos(tokens, sig_list[cur_s - 2])
                )
            )
            if _is_select_head_modifier_end_lex(tokens, cur_s - 1, sig_list):
                cur_val = False
            elif (prev_k == "OTHER" and not _is_postfix_factorial_lex(tokens, cur_s - 1, sig_list)) or (
                prev_k == "OP" and prev_t not in (")", "]", "}", ";") and not is_star_wildcard
            ):
                cur_val = False
            elif prev_k == "IDENT" and not _is_non_kw_lex_pos(tokens, prev_idx):
                pw = prev_t.upper()
                if pw in ("IN", "GLOBAL"):
                    cur_val = False
                elif pw in ("DISTINCT", "ALL"):
                    is_setop_mod = (
                        cur_s >= 2
                        and tokens[sig_list[cur_s - 2]][0] == "IDENT"
                        and tokens[sig_list[cur_s - 2]][1].upper() in ("UNION", "INTERSECT", "EXCEPT")
                        and not _is_non_kw_lex_pos(tokens, sig_list[cur_s - 2])
                    )
                    if not is_setop_mod:
                        cur_val = False
                elif _is_lex_prev_word_operator(tokens, cur_s - 1, sig_list, tokens[sig_list[cur_s]]):
                    is_bare_table_alias = (
                        cur_s >= 3
                        and tokens[sig_list[cur_s - 2]][0] in ("IDENT", "DQUOTE")
                        and tokens[sig_list[cur_s - 3]][0] == "IDENT"
                        and tokens[sig_list[cur_s - 3]][1].upper() in ("FROM", "JOIN")
                        and not _is_non_kw_lex_pos(tokens, sig_list[cur_s - 3])
                        and not _is_lex_distinct_from(tokens, cur_s - 3, sig_list)
                    )
                    if not is_bare_table_alias:
                        cur_val = False
        while True:
            co_memo[cur_s] = cur_val
            if cur_s >= s_pos:
                return cur_val
            cur_s += 1
            cur_val = not cur_val

    def _is_non_kw_lex_pos(tokens: list[tuple[str, str]], idx: int) -> bool:
        if idx <= 0 or not _has_any_non_kw_sql:
            return False
        cached_m = _lex_meta_cache.get(id(tokens))
        if cached_m is not None and cached_m[0] is tokens:
            sig_list, sig_pos_map, as_run, _, _, non_kw_memo = (
                cached_m[1],
                cached_m[2],
                cached_m[3],
                cached_m[4],
                cached_m[5],
                cached_m[6],
            )
        else:
            sig_list, sig_pos_map, as_run, _, _, non_kw_memo = _get_lex_meta(tokens)
        s_pos = sig_pos_map.get(idx)
        if s_pos is None or s_pos == 0:
            return False
        cached = non_kw_memo.get(s_pos)
        if cached is not None:
            return cached
        prev_tok_idx = sig_list[s_pos - 1]
        if _is_dot_or_dcolon_lex(tokens, prev_tok_idx):
            non_kw_memo[s_pos] = True
            return True
        r = as_run[s_pos]
        if r == 0:
            non_kw_memo[s_pos] = False
            return False
        p_s = s_pos - 1 - r
        if p_s > 0 and p_s not in non_kw_memo and as_run[p_s] > 0:
            chain: list[int] = []
            cur_s = p_s
            while cur_s > 0 and cur_s not in non_kw_memo and as_run[cur_s] > 0:
                chain.append(cur_s)
                cur_s = cur_s - 1 - as_run[cur_s]
            for c_s in reversed(chain):
                c_r = as_run[c_s]
                c_p_s = c_s - 1 - c_r
                c_p_tok = sig_list[c_p_s] if c_p_s >= 0 else None
                if _cannot_end_aliasable_lex(tokens, c_p_tok, c_r, sig_list[c_s]):
                    c_r -= 1
                non_kw_memo[c_s] = (c_r % 2) == 1
        p_tok = sig_list[p_s] if p_s >= 0 else None
        if _cannot_end_aliasable_lex(tokens, p_tok, r, idx):
            r -= 1
        res = (r % 2) == 1
        non_kw_memo[s_pos] = res
        return res

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
            _has_any_dot_sql
            and s_pos + 2 < len(sig_list)
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

    _lex_on_op_candidate_words = _ON_EXPR_PREV_WORDS | frozenset(
        {"SELECT", "WHERE", "PREWHERE", "HAVING", "QUALIFY", "DISTINCT", "ALL"}
    )

    def _lex_prev_word_op_step(
        tokens: list[tuple[str, str]], prev_s_pos: int, sig_list: list[int], nxt_sig: tuple[str, str] | None
    ) -> tuple[bool | None, int]:
        prev_idx = sig_list[prev_s_pos]
        pw = tokens[prev_idx][1].upper()
        if pw not in _ON_EXPR_PREV_WORDS or _is_non_kw_lex_pos(tokens, prev_idx):
            return False, -1
        if pw == "FROM":
            return _is_lex_distinct_from(tokens, prev_s_pos, sig_list), -1
        if pw == "TO":
            return (
                prev_s_pos >= 1
                and tokens[sig_list[prev_s_pos - 1]][0] == "IDENT"
                and tokens[sig_list[prev_s_pos - 1]][1].upper() == "SIMILAR"
                and not _is_non_kw_lex_pos(tokens, sig_list[prev_s_pos - 1])
            ), -1
        if pw == "ZONE":
            return (
                prev_s_pos >= 2
                and tokens[sig_list[prev_s_pos - 1]][0] == "IDENT"
                and tokens[sig_list[prev_s_pos - 1]][1].upper() == "TIME"
                and not _is_non_kw_lex_pos(tokens, sig_list[prev_s_pos - 1])
                and tokens[sig_list[prev_s_pos - 2]][0] == "IDENT"
                and tokens[sig_list[prev_s_pos - 2]][1].upper() == "AT"
                and not _is_non_kw_lex_pos(tokens, sig_list[prev_s_pos - 2])
            ), -1
        if pw == "INTERVAL":
            return (
                nxt_sig is not None
                and nxt_sig[0] == "IDENT"
                and nxt_sig[1].upper() in _SQL_INTERVAL_UNITS
                and prev_s_pos + 2 < len(sig_list)
                and not _is_non_kw_lex_pos(tokens, sig_list[prev_s_pos + 2])
            ), -1
        if pw == "ESCAPE":
            return (False, -1) if prev_s_pos < 1 else (None, prev_s_pos - 1)
        if pw in ("SYMMETRIC", "ASYMMETRIC"):
            return (
                prev_s_pos >= 1
                and tokens[sig_list[prev_s_pos - 1]][0] == "IDENT"
                and tokens[sig_list[prev_s_pos - 1]][1].upper() == "BETWEEN"
                and not _is_non_kw_lex_pos(tokens, sig_list[prev_s_pos - 1])
            ), -1
        if pw == "NOT":
            return True, -1
        if pw == "CASE":
            if not _has_matching_case_when_end_lex(tokens, prev_s_pos, sig_list):
                return False, -1
            first_after = tokens[sig_list[prev_s_pos + 1]] if prev_s_pos + 1 < len(sig_list) else None
            if first_after is None:
                return False, -1
            if (
                first_after[0] == "IDENT"
                and first_after[1].upper() == "WHEN"
                and not _is_non_kw_lex_pos(tokens, sig_list[prev_s_pos + 1])
            ):
                return True, -1
            if first_after[0] != "IDENT":
                return True, -1
            if nxt_sig is None:
                return False, -1
            if (
                nxt_sig in (("OP", "."), ("OP", "("), ("OP", "["))
                or nxt_sig[0] in ("OTHER", "SUBQ")
                or (nxt_sig[0] == "OP" and nxt_sig[1] not in ("(", ")", "[", "]", "{", "}", ",", ";"))
            ):
                return True, -1
            return (
                nxt_sig[0] == "IDENT"
                and nxt_sig[1].upper() in _ON_EXPR_NEXT_WORDS
                and (prev_s_pos + 2 >= len(sig_list) or not _is_non_kw_lex_pos(tokens, sig_list[prev_s_pos + 2]))
            ), -1
        if pw == "ON":
            return (True, -1) if prev_s_pos == 0 else (None, prev_s_pos - 1)
        if pw == "WHEN":
            if (
                prev_s_pos >= 1
                and tokens[sig_list[prev_s_pos - 1]][0] == "IDENT"
                and tokens[sig_list[prev_s_pos - 1]][1].upper() == "CASE"
            ):
                return (not _is_non_kw_lex_pos(tokens, sig_list[prev_s_pos - 1])), -1
            return (False, -1) if prev_s_pos < 1 else (None, prev_s_pos - 1)
        if (
            pw in ("IN", "LIKE", "ILIKE", "RLIKE", "REGEXP", "GLOB", "SIMILAR", "BETWEEN")
            and prev_s_pos >= 2
            and tokens[sig_list[prev_s_pos - 1]][0] == "IDENT"
            and tokens[sig_list[prev_s_pos - 1]][1].upper() == "NOT"
            and not _is_non_kw_lex_pos(tokens, sig_list[prev_s_pos - 1])
        ):
            return None, prev_s_pos - 2
        return (False, -1) if prev_s_pos < 1 else (None, prev_s_pos - 1)

    def _eval_lex_on_op_and_fact(
        tokens: list[tuple[str, str]], start_kind: str, start_s_pos: int, sig_list: list[int]
    ) -> bool:
        _, _, as_run, _, _, non_kw_memo, case_state = _get_lex_meta_entry(tokens)
        on_op_memo: dict[int, bool] = case_state["on_op"]
        fact_memo: dict[int, bool] = case_state["fact"]
        if start_kind == "on_op":
            if start_s_pos < 0:
                return True
            cached = on_op_memo.get(start_s_pos)
            if cached is not None:
                return cached
        else:
            cached = fact_memo.get(start_s_pos)
            if cached is not None:
                return cached

        n_sig = len(sig_list)
        stack: list[tuple[str, int]] = [(start_kind, start_s_pos)]
        while stack:
            kind, sp = stack.pop()
            if kind == "fact":
                if sp in fact_memo:
                    continue
                if sp <= 0 or sp >= n_sig or tokens[sig_list[sp]] != ("OTHER", "!"):
                    fact_memo[sp] = False
                    continue
                k_s = sp
                while k_s > 0 and tokens[sig_list[k_s]] == ("OTHER", "!") and k_s not in fact_memo:
                    k_s -= 1
                if k_s in fact_memo:
                    run_val = fact_memo[k_s]
                    fill_start = k_s + 1
                elif tokens[sig_list[k_s]] == ("OTHER", "!"):
                    run_val = False
                    fact_memo[0] = False
                    on_op_memo[0] = True
                    fill_start = 1
                elif _is_select_head_signed_top_end_lex(tokens, k_s, sig_list):
                    run_val = True
                    fill_start = k_s + 1
                elif k_s in on_op_memo:
                    run_val = not on_op_memo[k_s]
                    fill_start = k_s + 1
                else:
                    stack.append(("fact", sp))
                    stack.append(("on_op", k_s))
                    continue
                for j_s in range(fill_start, sp + 1):
                    fact_memo[j_s] = run_val
                    on_op_memo[j_s] = not run_val
                continue

            if sp < 0 or sp in on_op_memo:
                continue
            if _has_select_head_mod_sql and _is_select_head_modifier_end_lex(tokens, sp, sig_list):
                on_op_memo[sp] = True
                continue
            tok_idx = sig_list[sp]
            tk, tt = tokens[tok_idx]
            if tk == "OTHER":
                if tt == "!":
                    if sp not in fact_memo:
                        stack.append(("on_op", sp))
                        stack.append(("fact", sp))
                        continue
                    on_op_memo[sp] = not fact_memo[sp]
                else:
                    on_op_memo[sp] = True
                continue
            if tk == "OP" and tt not in (")", "]", "}"):
                if tt == "*" and sp >= 1 and tokens[sig_list[sp - 1]] == ("OP", "."):
                    on_op_memo[sp] = False
                else:
                    on_op_memo[sp] = True
                continue
            if tk != "IDENT":
                on_op_memo[sp] = False
                continue
            wu = tt.upper()
            if wu not in _lex_on_op_candidate_words:
                on_op_memo[sp] = False
                continue
            if sp not in non_kw_memo and as_run[sp] > 0:
                cur_p = sp - 1 - as_run[sp]
                needed: list[int] = []
                while cur_p >= 0:
                    if cur_p not in on_op_memo:
                        needed.append(cur_p)
                    if cur_p > 0 and (cur_p - 1) not in on_op_memo and tokens[sig_list[cur_p]][0] == "IDENT":
                        needed.append(cur_p - 1)
                    if cur_p in non_kw_memo or as_run[cur_p] == 0:
                        break
                    cur_p = cur_p - 1 - as_run[cur_p]
                if needed:
                    stack.append(("on_op", sp))
                    for np in needed:
                        stack.append(("on_op", np))
                    continue
            if _is_non_kw_lex_pos(tokens, tok_idx):
                on_op_memo[sp] = False
                continue
            if wu in ("SELECT", "WHERE", "PREWHERE", "HAVING", "QUALIFY"):
                if sp == 0:
                    on_op_memo[sp] = True
                    continue
                prev_k, prev_t = tokens[sig_list[sp - 1]]
                if prev_k == "OTHER":
                    if prev_t == "!":
                        if (sp - 1) not in fact_memo:
                            stack.append(("on_op", sp))
                            stack.append(("fact", sp - 1))
                            continue
                        on_op_memo[sp] = fact_memo[sp - 1]
                    else:
                        on_op_memo[sp] = False
                    continue
                if prev_k == "OP" and prev_t not in (")", "]", "}", ";"):
                    on_op_memo[sp] = False
                    continue
                if prev_k == "IDENT":
                    if (sp - 1) not in non_kw_memo and as_run[sp - 1] > 0:
                        p_prev = (sp - 1) - 1 - as_run[sp - 1]
                        if p_prev >= 0 and p_prev not in on_op_memo:
                            stack.append(("on_op", sp))
                            stack.append(("on_op", p_prev))
                            continue
                    res_w, dep_w = _lex_prev_word_op_step(tokens, sp - 1, sig_list, tokens[tok_idx])
                    if res_w is None:
                        if dep_w not in on_op_memo:
                            stack.append(("on_op", sp))
                            stack.append(("on_op", dep_w))
                            continue
                        res_w = not on_op_memo[dep_w]
                    on_op_memo[sp] = not res_w
                    continue
                on_op_memo[sp] = True
                continue
            if wu in ("DISTINCT", "ALL"):
                on_op_memo[sp] = (
                    sp >= 1
                    and tokens[sig_list[sp - 1]][0] == "IDENT"
                    and tokens[sig_list[sp - 1]][1].upper() in ("SELECT", "UNION", "INTERSECT", "EXCEPT")
                    and not _is_non_kw_lex_pos(tokens, sig_list[sp - 1])
                ) or (
                    wu == "ALL"
                    and sp + 1 < n_sig
                    and (tokens[sig_list[sp + 1]] == ("OP", "(") or tokens[sig_list[sp + 1]][0] == "SUBQ")
                )
                continue
            after_operand = tokens[sig_list[sp + 2]] if sp + 2 < n_sig else None
            if sp >= 1 and (sp - 1) not in non_kw_memo and as_run[sp - 1] > 0:
                p_prev2 = (sp - 1) - 1 - as_run[sp - 1]
                if p_prev2 >= 0 and p_prev2 not in on_op_memo:
                    stack.append(("on_op", sp))
                    stack.append(("on_op", p_prev2))
                    continue
            res_w, dep_w = _lex_prev_word_op_step(tokens, sp, sig_list, after_operand)
            if res_w is None:
                if dep_w not in on_op_memo:
                    stack.append(("on_op", sp))
                    stack.append(("on_op", dep_w))
                    continue
                res_w = not on_op_memo[dep_w]
            on_op_memo[sp] = res_w

        return on_op_memo[start_s_pos] if start_kind == "on_op" else fact_memo[start_s_pos]

    def _is_lex_on_operator_tok(tokens: list[tuple[str, str]], s_pos: int, sig_list: list[int]) -> bool:
        if s_pos < 0:
            return True
        return _eval_lex_on_op_and_fact(tokens, "on_op", s_pos, sig_list)

    def _is_lex_prev_word_operator(
        tokens: list[tuple[str, str]], prev_s_pos: int, sig_list: list[int], nxt_sig: tuple[str, str] | None
    ) -> bool:
        res, dep = _lex_prev_word_op_step(tokens, prev_s_pos, sig_list, nxt_sig)
        if res is not None:
            return res
        return not _eval_lex_on_op_and_fact(tokens, "on_op", dep, sig_list)

    def _is_lex_multi_token_post_on_exit(tokens: list[tuple[str, str]], s_pos: int, sig_list: list[int]) -> bool:
        wu = tokens[sig_list[s_pos]][1].upper()
        if wu not in ("GROUP", "ORDER", "FETCH", "INTO", "UNION", "INTERSECT", "EXCEPT"):
            return False
        if wu == "GROUP" and _is_lex_within_group(tokens, s_pos, sig_list):
            return False
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
        wu = tokens[sig_list[s_pos]][1].upper()
        if wu == "GROUP" and _is_lex_within_group(tokens, s_pos, sig_list):
            return True
        if _is_lex_multi_token_post_on_exit(tokens, s_pos, sig_list):
            return False
        prev_idx = sig_list[s_pos - 1] if s_pos > 0 else None
        prev_sig = tokens[prev_idx] if prev_idx is not None else None
        nxt_sig = tokens[sig_list[s_pos + 1]] if s_pos + 1 < len(sig_list) else None
        if prev_sig is not None:
            is_prev_qual_star = prev_sig == ("OP", "*") and s_pos >= 2 and tokens[sig_list[s_pos - 2]] == ("OP", ".")
            if (prev_sig[0] == "OTHER" and not _is_postfix_factorial_lex(tokens, s_pos - 1, sig_list)) or (
                prev_sig[0] == "OP" and prev_sig[1] not in (")", "]", "}", ";") and not is_prev_qual_star
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
                "JOIN",
                "FROM",
                "TABLE",
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
                and wu
                not in ("SELECT", "WHERE", "PREWHERE", "HAVING", "QUALIFY", "LIMIT", "OFFSET", "JOIN", "FROM", "TABLE")
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

    def _is_lex_star_except(tokens: list[tuple[str, str]], s_pos: int, sig_list: list[int]) -> bool:
        return (
            s_pos > 0
            and s_pos + 1 < len(sig_list)
            and tokens[sig_list[s_pos]][0] == "IDENT"
            and tokens[sig_list[s_pos]][1].upper() == "EXCEPT"
            and tokens[sig_list[s_pos - 1]] == ("OP", "*")
            and tokens[sig_list[s_pos + 1]] == ("OP", "(")
        )

    def _find_matching_paren(tokens: list[tuple[str, str]], open_idx: int) -> int:
        _, _, _, match_by_tok, _, _ = _get_lex_meta(tokens)
        close_idx = match_by_tok.get(open_idx, -1)
        return close_idx if close_idx > open_idx else len(tokens) - 1

    def _has_enclosing_parens(tokens: list[tuple[str, str]]) -> tuple[int, int] | None:
        sig, _, _, match_by_tok, _, _ = _get_lex_meta(tokens)
        if len(sig) >= 2 and tokens[sig[0]] == ("OP", "(") and match_by_tok.get(sig[0]) == sig[-1]:
            return sig[0], sig[-1]
        return None

    def _peel_enclosing_parens(tokens: list[tuple[str, str]]) -> tuple[list[tuple[str, str]], str, str]:
        sig, _, _, match_by_tok, _, _ = _get_lex_meta(tokens)
        lo_s = 0
        hi_s = len(sig) - 1
        if hi_s < 1 or tokens[sig[0]] != ("OP", "(") or match_by_tok.get(sig[0]) != sig[-1]:
            return tokens, "", ""
        while lo_s < hi_s and tokens[sig[lo_s]] == ("OP", "(") and match_by_tok.get(sig[lo_s]) == sig[hi_s]:
            lo_s += 1
            hi_s -= 1
        start_tok = sig[lo_s - 1] + 1
        end_tok = sig[hi_s + 1]
        prefix_s = "".join(tk for _, tk in tokens[:start_tok])
        suffix_s = "".join(tk for _, tk in tokens[end_tok:])
        return tokens[start_tok:end_tok], prefix_s, suffix_s

    def _is_subquery_tokens(inner_tokens: list[tuple[str, str]]) -> bool:
        cur, _, _ = _peel_enclosing_parens(inner_tokens)
        while True:
            sig, _, _, match_by_tok, _, _ = _get_lex_meta(cur)
            if not sig:
                return False
            k0, t0 = cur[sig[0]]
            if k0 == "IDENT" and t0.upper() in ("SELECT", "WITH", "FROM", "TABLE"):
                return True
            if k0 == "OP" and t0 == "(":
                close_first = match_by_tok.get(sig[0], len(cur) - 1)
                if close_first < sig[-1]:
                    j = close_first + 1
                    while j < len(cur) and cur[j][0] in ("WS", "COMMENT"):
                        j += 1
                    if (
                        j < len(cur)
                        and cur[j][0] == "IDENT"
                        and cur[j][1].upper() in ("UNION", "INTERSECT", "EXCEPT", "ORDER", "LIMIT", "OFFSET", "FETCH")
                    ):
                        cur, _, _ = _peel_enclosing_parens(cur[sig[0] + 1 : close_first])
                        continue
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
        if not _has_any_quote_or_comment_sql or not any(k in ("COMMENT", "DQUOTE", "STR") for k, _ in segment_tokens):
            seg_sql = "".join([t for _, t in segment_tokens])
        else:
            parts: list[str] = []
            sig_idxs, sig_pos_map, _, _, _, _ = _get_lex_meta(segment_tokens)
            for idx, (k, t) in enumerate(segment_tokens):
                if k == "COMMENT":
                    s_idx = len(saved_lits)
                    saved_lits.append(t)
                    parts.append(f"__fla_str_lit_{lit_nonce}_{s_idx}__")
                elif k == "DQUOTE":
                    s_pos = sig_pos_map.get(idx, -1)
                    next_sig = (
                        segment_tokens[sig_idxs[s_pos + 1]] if (s_pos >= 0 and s_pos + 1 < len(sig_idxs)) else None
                    )
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

        def _scan_qual_prefix_start(src: str, cf_start: int, min_pos: int) -> int:
            qual_start = cf_start
            pos = cf_start
            while pos > min_pos:
                j = pos
                while j > min_pos and src[j - 1].isspace():
                    j -= 1
                if j <= min_pos or src[j - 1] != ".":
                    break
                j -= 1
                while j > min_pos and src[j - 1].isspace():
                    j -= 1
                if j <= min_pos:
                    break
                ch = src[j - 1]
                if ch in ('"', "`"):
                    q_ch = ch
                    k_q = j - 2
                    found_q = -1
                    while k_q >= min_pos:
                        if src[k_q] == q_ch:
                            if k_q > min_pos and src[k_q - 1] == q_ch:
                                k_q -= 2
                                continue
                            found_q = k_q
                            break
                        k_q -= 1
                    if found_q < 0 or found_q == j - 2:
                        break
                    qual_start = found_q
                    pos = found_q
                elif ch.isalnum() or ch == "_":
                    k_i = j - 1
                    while k_i > min_pos and (src[k_i - 1].isalnum() or src[k_i - 1] == "_"):
                        k_i -= 1
                    if not (src[k_i].isalpha() or src[k_i] == "_"):
                        break
                    qual_start = k_i
                    pos = k_i
                else:
                    break
            return qual_start

        def _match_rev_bool_prefix(src: str, qual_start: int, min_pos: int) -> tuple[int, str] | None:
            j = qual_start
            while j > min_pos and src[j - 1].isspace():
                j -= 1
            op_ne = False
            if j - 2 >= min_pos and src[j - 2 : j] in ("!=", "<>"):
                op_ne = True
                j -= 2
            elif j - 1 >= min_pos and src[j - 1] == "=":
                if j - 2 >= min_pos and src[j - 2] in ("!", "<", ">"):
                    return None
                j -= 1
            else:
                return None
            while j > min_pos and src[j - 1].isspace():
                j -= 1
            low_tail = src[max(min_pos, j - 7) : j].lower()
            lit_len = 0
            is_true = False
            for lit, lit_is_true in (("'true'", True), ("'false'", False), ("true", True), ("false", False)):
                if low_tail.endswith(lit):
                    lit_len = len(lit)
                    is_true = lit_is_true
                    break
            if lit_len == 0:
                return None
            lit_start = j - lit_len
            if lit_start > 0:
                prev_c = src[lit_start - 1]
                if prev_c.isalnum() or prev_c in ("_", ".", '"', "'", "`"):
                    return None
            if op_ne:
                return (lit_start, "NOT IN ('true', '1')" if is_true else "IN ('true', '1')")
            return (lit_start, "IN ('true', '1')" if is_true else "IN ('false', '0', '')")

        seg_low_check = seg_sql.lower()
        has_cf_lookup = touches_request and "custom_fields[" in seg_low_check
        if has_cf_lookup:
            _bool_rules_re = tuple((re.compile(rf"\s*{pat}", re.IGNORECASE), repl) for pat, repl in _bool_rules)
            for b_field in sorted(_REQUEST_BOOLEAN_CF_FIELDS):
                if f"custom_fields['{b_field}']" not in seg_low_check:
                    continue
                cf_re = re.compile(rf"(?<![A-Za-z0-9_])custom_fields\['{re.escape(b_field)}'\]", re.IGNORECASE)
                out_fwd: list[str] = []
                last_end = 0
                for m_cf in cf_re.finditer(seg_sql):
                    if m_cf.start() < last_end:
                        continue
                    matched_repl: str | None = None
                    matched_end = -1
                    for rule_re, repl in _bool_rules_re:
                        fm = rule_re.match(seg_sql, m_cf.end())
                        if fm is not None:
                            matched_repl = repl
                            matched_end = fm.end()
                            break
                    if matched_repl is not None:
                        q_start = _scan_qual_prefix_start(seg_sql, m_cf.start(), last_end)
                        qual = seg_sql[q_start : m_cf.start()]
                        out_fwd.append(seg_sql[last_end:q_start])
                        out_fwd.append(f"lower({qual}custom_fields['{b_field}']) {matched_repl}")
                        last_end = matched_end
                if out_fwd:
                    out_fwd.append(seg_sql[last_end:])
                    seg_sql = "".join(out_fwd)

                out_rev: list[str] = []
                last_end = 0
                for m_cf in cf_re.finditer(seg_sql):
                    if m_cf.start() < last_end:
                        continue
                    q_start = _scan_qual_prefix_start(seg_sql, m_cf.start(), last_end)
                    rev_hit = _match_rev_bool_prefix(seg_sql, q_start, last_end)
                    if rev_hit is not None:
                        rev_start, r_repl = rev_hit
                        qual = seg_sql[q_start : m_cf.start()]
                        out_rev.append(seg_sql[last_end:rev_start])
                        out_rev.append(f"lower({qual}custom_fields['{b_field}']) {r_repl}")
                        last_end = m_cf.end()
                if out_rev:
                    out_rev.append(seg_sql[last_end:])
                    seg_sql = "".join(out_rev)

        _alias_map = alias_schema_map or {}
        _physical_req_cols = _REQUEST_TYPED_COLUMNS | {
            "timestamp",
            "event_timestamp",
            "ip",
            "client_ip",
            "custom_fields",
        }

        def _qual_matches_derived(raw_qual: str, col_name: str, *, want_bool: bool = False) -> bool:
            if not (derived_str_cols and (unqualified_derived or _alias_map)):
                return False
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

        if derived_str_cols and (unqualified_derived or _alias_map):
            active_derived_bool_cols = frozenset(
                d_col for d_col in derived_str_cols if d_col in _REQUEST_BOOLEAN_CF_FIELDS or d_col in derived_bool_cols
            )
            if active_derived_bool_cols:
                _dc_bool_rules_re = tuple((re.compile(rf"\s*{pat}", re.IGNORECASE), repl) for pat, repl in _bool_rules)
                seg_len_dc = len(seg_sql)
                out_dc: list[str] = []
                last_end_dc = 0
                for m_id in SAFE_IDENT_RE.finditer(seg_sql):
                    col_raw = m_id.group(0)
                    col_low = col_raw.lower()
                    if col_low not in active_derived_bool_cols:
                        continue
                    col_start = m_id.start()
                    col_end = m_id.end()
                    if col_start < last_end_dc:
                        continue
                    j_after = col_end
                    while j_after < seg_len_dc and seg_sql[j_after].isspace():
                        j_after += 1
                    if j_after < seg_len_dc and seg_sql[j_after] in (".", "("):
                        continue
                    j_pre = col_start
                    while j_pre > last_end_dc and seg_sql[j_pre - 1].isspace():
                        j_pre -= 1
                    q_start = col_start
                    q_part = ""
                    if j_pre > last_end_dc and seg_sql[j_pre - 1] == ".":
                        j_qend = j_pre - 1
                        while j_qend > last_end_dc and seg_sql[j_qend - 1].isspace():
                            j_qend -= 1
                        if j_qend <= last_end_dc:
                            continue
                        q_ch = seg_sql[j_qend - 1]
                        if q_ch in ('"', "`"):
                            k_q = j_qend - 2
                            found_q = -1
                            while k_q >= last_end_dc:
                                if seg_sql[k_q] == q_ch:
                                    if k_q > last_end_dc and seg_sql[k_q - 1] == q_ch:
                                        k_q -= 2
                                        continue
                                    found_q = k_q
                                    break
                                k_q -= 1
                            if found_q < 0 or found_q == j_qend - 2:
                                continue
                            q_start = found_q
                        elif q_ch.isalnum() or q_ch == "_":
                            k_i = j_qend - 1
                            while k_i > last_end_dc and (seg_sql[k_i - 1].isalnum() or seg_sql[k_i - 1] == "_"):
                                k_i -= 1
                            if not (seg_sql[k_i].isalpha() or seg_sql[k_i] == "_"):
                                continue
                            q_start = k_i
                        else:
                            continue
                        if q_start > 0:
                            pc_q = seg_sql[q_start - 1]
                            if pc_q.isalnum() or pc_q in ("_", ".", '"', "'", "`"):
                                continue
                        q_part = seg_sql[q_start:col_start]
                    elif col_start > 0:
                        pc_c = seg_sql[col_start - 1]
                        if pc_c.isalnum() or pc_c in ("_", ".", '"', "'", "`"):
                            continue
                    if not _qual_matches_derived(q_part, col_low, want_bool=True):
                        continue
                    matched_repl_dc: str | None = None
                    matched_end_dc = -1
                    if j_after < seg_len_dc and seg_sql[j_after] in "iInN!<=":
                        for rule_re_dc, repl_dc in _dc_bool_rules_re:
                            fm_dc = rule_re_dc.match(seg_sql, col_end)
                            if fm_dc is not None:
                                matched_repl_dc = repl_dc
                                matched_end_dc = fm_dc.end()
                                break
                    if matched_repl_dc is not None:
                        out_dc.append(seg_sql[last_end_dc:q_start])
                        out_dc.append(f"lower({q_part}{col_raw}) {matched_repl_dc}")
                        last_end_dc = matched_end_dc
                        continue
                    rev_hit_dc = _match_rev_bool_prefix(seg_sql, q_start, last_end_dc)
                    if rev_hit_dc is not None:
                        rev_start_dc, r_repl_dc = rev_hit_dc
                        out_dc.append(seg_sql[last_end_dc:rev_start_dc])
                        out_dc.append(f"lower({q_part}{col_raw}) {r_repl_dc}")
                        last_end_dc = col_end
                if out_dc:
                    out_dc.append(seg_sql[last_end_dc:])
                    seg_sql = "".join(out_dc)

        if (has_cf_lookup or (derived_str_cols and (unqualified_derived or _alias_map))) and (
            re.search(r"[+\-*/%]|(?<![A-Za-z0-9_'\"`])[0-9]|\bcast\b", seg_sql, flags=re.IGNORECASE) is not None
        ):
            lex_toks = [
                (m.lastgroup or "OTHER", m.group(0), m.start(), m.end()) for m in _CONSOLE_SQL_LEX_RE.finditer(seg_sql)
            ]
            sig_c = [idx_t for idx_t, tk_t in enumerate(lex_toks) if tk_t[0] not in ("WS", "COMMENT")]
            _non_call_open_kw = _unary_kw | frozenset(
                {"IN", "BETWEEN", "IS", "DISTINCT", "FROM", "AS", "JOIN", "USING", "EXISTS", "ALL", "ANY", "SOME"}
            )
            _post_num_kw = frozenset(
                {
                    "AND",
                    "OR",
                    "XOR",
                    "ORDER",
                    "GROUP",
                    "HAVING",
                    "LIMIT",
                    "OFFSET",
                    "FETCH",
                    "UNION",
                    "INTERSECT",
                    "EXCEPT",
                    "WINDOW",
                    "QUALIFY",
                    "SETTINGS",
                    "FORMAT",
                    "PREWHERE",
                    "SAMPLE",
                    "INTO",
                    "FOR",
                    "WHEN",
                    "THEN",
                    "ELSE",
                    "END",
                    "AS",
                    "FROM",
                    "WHERE",
                    "ON",
                }
            )
            _lit_prefix = f"__fla_str_lit_{lit_nonce}_"
            _numeric_cast_types = frozenset(
                {
                    "INT",
                    "INTEGER",
                    "BIGINT",
                    "SMALLINT",
                    "TINYINT",
                    "MEDIUMINT",
                    "INT8",
                    "INT16",
                    "INT32",
                    "INT64",
                    "INT128",
                    "INT256",
                    "UINT8",
                    "UINT16",
                    "UINT32",
                    "UINT64",
                    "UINT128",
                    "UINT256",
                    "FLOAT",
                    "FLOAT32",
                    "FLOAT64",
                    "DOUBLE",
                    "REAL",
                    "DECIMAL",
                    "NUMERIC",
                    "NUMBER",
                    "HUGEINT",
                    "UHUGEINT",
                    "SIGNED",
                    "UNSIGNED",
                }
            )
            paren_match_sp: dict[int, int] = {}
            _p_stack_c: list[int] = []
            for _cur_sp_i, _tok_c_i in enumerate(sig_c):
                _ck_c, _ct_c, _, _ = lex_toks[_tok_c_i]
                if _ck_c == "OP" and _ct_c == "(":
                    _p_stack_c.append(_cur_sp_i)
                    if len(_p_stack_c) > _MAX_PAREN_DEPTH:
                        raise SQLValidationError("nesting_depth_exceeded", "SQL nesting depth limit exceeded")
                elif _ck_c == "OP" and _ct_c == ")":
                    if _p_stack_c:
                        _open_c = _p_stack_c.pop()
                        paren_match_sp[_open_c] = _cur_sp_i
                        paren_match_sp[_cur_sp_i] = _open_c

            def _match_paren_sp(open_sp: int) -> int:
                ci = paren_match_sp.get(open_sp, -1)
                return ci if ci > open_sp else -1

            case_info_sp: dict[int, tuple[int, list[tuple[int, int]]]] = {}
            _case_frames_c: list[list[list[Any]]] = [[]]
            for _cur_sp_i, _tok_c_i in enumerate(sig_c):
                _ck_c, _ct_c, _, _ = lex_toks[_tok_c_i]
                if (_ck_c, _ct_c) == ("OP", "("):
                    if _match_paren_sp(_cur_sp_i) <= _cur_sp_i:
                        _case_frames_c[-1].clear()
                    _case_frames_c.append([])
                    continue
                if (_ck_c, _ct_c) == ("OP", ")"):
                    if len(_case_frames_c) > 1:
                        _case_frames_c.pop()
                    else:
                        _case_frames_c[0].clear()
                    continue
                if _ck_c == "IDENT":
                    _cu_c = _ct_c.upper()
                    if _cu_c == "CASE":
                        _case_frames_c[-1].append([_cur_sp_i, -1, []])
                    elif _cu_c == "END":
                        if _case_frames_c[-1]:
                            _c_open, _c_bstart, _c_spans = _case_frames_c[-1].pop()
                            if _c_bstart >= 0:
                                _c_spans.append((_c_bstart, _cur_sp_i - 1))
                            case_info_sp[_c_open] = (_cur_sp_i, _c_spans)
                    elif _cu_c in ("WHEN", "ELSE"):
                        if _case_frames_c[-1]:
                            _c_top = _case_frames_c[-1][-1]
                            if _c_top[1] >= 0:
                                _c_top[2].append((_c_top[1], _cur_sp_i - 1))
                                _c_top[1] = -1
                            if _cu_c == "ELSE":
                                _c_top[1] = _cur_sp_i + 1
                    elif _cu_c == "THEN":
                        if _case_frames_c[-1]:
                            _case_frames_c[-1][-1][1] = _cur_sp_i + 1
            case_num_memo: dict[int, int] = {}
            num_expr_memo: dict[tuple[int, bool], tuple[int, bool, bool] | None] = {}
            col_arith_memo: dict[tuple[int, bool], tuple[int, tuple[tuple[int, int, str], ...], bool] | None] = {}
            _plain_dec_re = re.compile(r"\d+(?:\.\d+)?")
            _plain_neg_dec_re = re.compile(r"-\d+(?:\.\d+)?")
            valid_col_starts: set[int] = set()
            for _vc_sp, _vc_ti in enumerate(sig_c):
                _vc_k, _vc_t, _, _ = lex_toks[_vc_ti]
                if _vc_k != "IDENT":
                    continue
                _vc_low = _vc_t.lower()
                if not ((touches_request and _vc_low == "custom_fields") or _vc_low in derived_str_cols):
                    continue
                _vc_cur = _vc_sp
                valid_col_starts.add(_vc_cur)
                while (
                    _vc_cur >= 2
                    and lex_toks[sig_c[_vc_cur - 1]][0:2] == ("OP", ".")
                    and lex_toks[sig_c[_vc_cur - 2]][0] in ("IDENT", "DQUOTE")
                ):
                    _vc_cur -= 2
                    valid_col_starts.add(_vc_cur)
                while _vc_cur >= 1 and lex_toks[sig_c[_vc_cur - 1]][0:2] == ("OP", "("):
                    _vc_cur -= 1
                    valid_col_starts.add(_vc_cur)

            def _is_arith_op_sp(op_idx: int) -> bool:
                if op_idx >= len(sig_c):
                    return False
                ok, ot, _, _ = lex_toks[sig_c[op_idx]]
                return (ok == "OP" and ot in ("+", "-", "*", "/")) or (ok == "OTHER" and ot == "%")

            def _is_call_before_sp(open_sp: int) -> bool:
                if open_sp <= 0:
                    return False
                pk, pt, _, _ = lex_toks[sig_c[open_sp - 1]]
                if pk == "DQUOTE":
                    return True
                if pk == "IDENT":
                    if open_sp >= 2 and lex_toks[sig_c[open_sp - 2]][0:2] == ("OP", "."):
                        return True
                    return pt.upper() not in _non_call_open_kw
                return False

            def _not_chained_after(end_sp: int) -> bool:
                if end_sp + 1 >= len(sig_c):
                    return True
                nk, nt, _, _ = lex_toks[sig_c[end_sp + 1]]
                return not (nk == "OP" and nt in (".", "[", "("))

            def _parse_str_col_sp(sp: int) -> tuple[int, str, bool] | None:
                if sp >= len(sig_c):
                    return None
                cur_sp = sp
                outer_close = -1
                expected_inner_end = -1
                had_parens = False
                while cur_sp < len(sig_c) and lex_toks[sig_c[cur_sp]][0:2] == ("OP", "("):
                    if _is_call_before_sp(cur_sp):
                        return None
                    close_sp = _match_paren_sp(cur_sp)
                    if close_sp <= cur_sp + 1 or not _not_chained_after(close_sp):
                        return None
                    if had_parens and close_sp != expected_inner_end:
                        return None
                    if not had_parens:
                        outer_close = close_sp
                    had_parens = True
                    expected_inner_end = close_sp - 1
                    cur_sp += 1
                if cur_sp >= len(sig_c):
                    return None
                if cur_sp > 0 and lex_toks[sig_c[cur_sp - 1]][0:2] == ("OP", "."):
                    return None
                q_sp = cur_sp
                while (
                    q_sp + 2 < len(sig_c)
                    and lex_toks[sig_c[q_sp]][0] in ("IDENT", "DQUOTE")
                    and not lex_toks[sig_c[q_sp]][1].startswith(_lit_prefix)
                    and lex_toks[sig_c[q_sp + 1]][0:2] == ("OP", ".")
                ):
                    q_sp += 2
                if q_sp >= len(sig_c):
                    return None
                qk, qt, _, q_end = lex_toks[sig_c[q_sp]]
                res_end = -1
                core_sql = ""
                if (
                    touches_request
                    and qk == "IDENT"
                    and qt.lower() == "custom_fields"
                    and q_sp + 3 < len(sig_c)
                    and lex_toks[sig_c[q_sp + 1]][0:2] == ("OP", "[")
                    and lex_toks[sig_c[q_sp + 2]][0] == "STR"
                    and cf_key_lit_re.fullmatch(lex_toks[sig_c[q_sp + 2]][1]) is not None
                    and lex_toks[sig_c[q_sp + 3]][0:2] == ("OP", "]")
                    and _not_chained_after(q_sp + 3)
                ):
                    core_sql = seg_sql[lex_toks[sig_c[cur_sp]][2] : lex_toks[sig_c[q_sp + 3]][3]]
                    res_end = q_sp + 3
                elif (
                    q_sp in (cur_sp, cur_sp + 2)
                    and qk == "IDENT"
                    and not qt.startswith(_lit_prefix)
                    and qt.lower() in derived_str_cols
                    and _not_chained_after(q_sp)
                ):
                    raw_qual = seg_sql[lex_toks[sig_c[cur_sp]][2] : lex_toks[sig_c[q_sp]][2]] if q_sp > cur_sp else ""
                    if _qual_matches_derived(raw_qual, qt.lower()):
                        core_sql = seg_sql[lex_toks[sig_c[cur_sp]][2] : q_end]
                        res_end = q_sp
                if res_end < 0:
                    return None
                if had_parens:
                    if res_end != expected_inner_end:
                        return None
                    return (outer_close, core_sql, True)
                return (res_end, core_sql, False)

            def _parse_numeric_cast_sp(sp: int) -> int:
                if sp + 1 >= len(sig_c) or lex_toks[sig_c[sp + 1]][0:2] != ("OP", "("):
                    return -1
                close_sp = _match_paren_sp(sp + 1)
                if close_sp <= sp + 3 or not _not_chained_after(close_sp):
                    return -1
                as_sp = -1
                cur = sp + 2
                while cur < close_sp:
                    ck, ct, _, _ = lex_toks[sig_c[cur]]
                    if (ck, ct) == ("OP", "("):
                        mc = _match_paren_sp(cur)
                        if mc <= cur:
                            return -1
                        cur = mc + 1
                        continue
                    if ck == "IDENT" and ct.upper() == "AS":
                        as_sp = cur
                        break
                    cur += 1
                if as_sp < 0 or as_sp + 1 >= close_sp:
                    return -1
                t_sp = as_sp + 1
                tk_t, tt_t, _, _ = lex_toks[sig_c[t_sp]]
                if tk_t != "IDENT":
                    return -1
                if tt_t.upper() == "NULLABLE" and t_sp + 2 < close_sp and lex_toks[sig_c[t_sp + 1]][0:2] == ("OP", "("):
                    t_sp += 2
                    tk_t, tt_t, _, _ = lex_toks[sig_c[t_sp]]
                    if tk_t != "IDENT":
                        return -1
                if tt_t.upper() in _numeric_cast_types:
                    return close_sp
                return -1

            def _parse_numeric_case_sp(
                sp: int,
                depth: int,
                parse_num_fn: Any,
            ) -> int:
                if depth > _MAX_PAREN_DEPTH:
                    return -1
                cached_case = case_num_memo.get(sp)
                if cached_case is not None:
                    return cached_case
                info = case_info_sp.get(sp)
                if info is None:
                    case_num_memo[sp] = -1
                    return -1
                end_sp, branch_spans = info
                if not branch_spans or not _not_chained_after(end_sp):
                    case_num_memo[sp] = -1
                    return -1
                saw_numeric = False
                for b_start, b_end in branch_spans:
                    if b_start > b_end:
                        case_num_memo[sp] = -1
                        return -1
                    if (
                        b_start == b_end
                        and lex_toks[sig_c[b_start]][0] == "IDENT"
                        and lex_toks[sig_c[b_start]][1].upper() == "NULL"
                    ):
                        continue
                    bm = parse_num_fn(b_start, atom_only=False, depth=depth + 1)
                    if bm is None or bm[0] != b_end:
                        case_num_memo[sp] = -1
                        return -1
                    saw_numeric = True
                res = end_sp if saw_numeric else -1
                case_num_memo[sp] = res
                return res

            def _parse_num_expr_sp(
                sp: int, *, atom_only: bool = False, depth: int = 0
            ) -> tuple[int, bool, bool] | None:
                if sp >= len(sig_c) or depth > _MAX_PAREN_DEPTH + 4:
                    return None
                memo_key = (sp, atom_only)
                if memo_key in num_expr_memo:
                    return num_expr_memo[memo_key]
                t_start = lex_toks[sig_c[sp]][2]
                cur_sp = sp
                sign_count = 0
                while cur_sp < len(sig_c) and lex_toks[sig_c[cur_sp]][0:2] in (("OP", "+"), ("OP", "-")):
                    sign_count += 1
                    cur_sp += 1
                if cur_sp >= len(sig_c):
                    num_expr_memo[memo_key] = None
                    return None
                tk0, tt0, _, t_end = lex_toks[sig_c[cur_sp]]
                atom_end: int
                plain_dec: bool
                has_sub = False
                if tk0 == "NUM":
                    if cur_sp > 0 and lex_toks[sig_c[cur_sp - 1]][0:2] == ("OP", "."):
                        num_expr_memo[memo_key] = None
                        return None
                    if cur_sp + 1 < len(sig_c) and lex_toks[sig_c[cur_sp + 1]][0:2] == ("OP", "."):
                        num_expr_memo[memo_key] = None
                        return None
                    atom_end = cur_sp
                    atom_sql = seg_sql[t_start:t_end]
                    plain_dec = bool(
                        (sign_count == 0 and _plain_dec_re.fullmatch(atom_sql))
                        or (sign_count == 1 and _plain_neg_dec_re.fullmatch(atom_sql))
                    )
                elif (tk0, tt0) == ("OP", "(") and not _is_call_before_sp(cur_sp):
                    close_sp = _match_paren_sp(cur_sp)
                    if close_sp <= cur_sp + 1 or not _not_chained_after(close_sp):
                        num_expr_memo[memo_key] = None
                        return None
                    inner_n = _parse_num_expr_sp(cur_sp + 1, atom_only=False, depth=depth + 1)
                    if inner_n is None or inner_n[0] != close_sp - 1:
                        num_expr_memo[memo_key] = None
                        return None
                    atom_end = close_sp
                    plain_dec = False
                    has_sub = inner_n[2]
                elif tk0 == "IDENT" and tt0.upper() in ("CAST", "TRY_CAST"):
                    cast_end = _parse_numeric_cast_sp(cur_sp)
                    if cast_end < 0:
                        num_expr_memo[memo_key] = None
                        return None
                    atom_end = cast_end
                    plain_dec = False
                    has_sub = True
                elif tk0 == "IDENT" and tt0.upper() == "CASE":
                    case_end = _parse_numeric_case_sp(cur_sp, depth + 1, _parse_num_expr_sp)
                    if case_end < 0:
                        num_expr_memo[memo_key] = None
                        return None
                    atom_end = case_end
                    plain_dec = False
                    has_sub = True
                else:
                    num_expr_memo[memo_key] = None
                    return None
                if atom_only:
                    res_a = (atom_end, plain_dec, has_sub)
                    num_expr_memo[memo_key] = res_a
                    return res_a
                while _is_arith_op_sp(atom_end + 1):
                    rhs_a = _parse_num_expr_sp(atom_end + 2, atom_only=True, depth=depth + 1)
                    if rhs_a is None:
                        num_expr_memo[memo_key] = None
                        return None
                    atom_end = rhs_a[0]
                    plain_dec = False
                    has_sub = has_sub or rhs_a[2]
                res_f = (atom_end, plain_dec, has_sub)
                num_expr_memo[memo_key] = res_f
                return res_f

            def _parse_col_arith_expr_sp(
                sp: int, *, atom_only: bool = False, depth: int = 0
            ) -> tuple[int, tuple[tuple[int, int, str], ...], bool] | None:
                if sp >= len(sig_c) or depth > _MAX_PAREN_DEPTH + 4:
                    return None
                memo_key = (sp, atom_only)
                if memo_key in col_arith_memo:
                    return col_arith_memo[memo_key]
                cur_sp = sp
                sign_count = 0
                while cur_sp < len(sig_c) and lex_toks[sig_c[cur_sp]][0:2] in (("OP", "+"), ("OP", "-")):
                    sign_count += 1
                    cur_sp += 1
                if cur_sp >= len(sig_c):
                    col_arith_memo[memo_key] = None
                    return None
                atom_spans: tuple[tuple[int, int, str], ...] = ()
                has_op = False
                atom_end: int
                col_a = _parse_str_col_sp(cur_sp) if cur_sp in valid_col_starts else None
                if col_a is not None:
                    atom_end = col_a[0]
                    atom_spans = ((lex_toks[sig_c[cur_sp]][2], lex_toks[sig_c[atom_end]][3], col_a[1]),)
                    if sign_count > 0:
                        has_op = True
                elif lex_toks[sig_c[cur_sp]][0:2] == ("OP", "(") and not _is_call_before_sp(cur_sp):
                    close_sp = _match_paren_sp(cur_sp)
                    if close_sp <= cur_sp + 1 or not _not_chained_after(close_sp):
                        col_arith_memo[memo_key] = None
                        return None
                    inner_ca = _parse_col_arith_expr_sp(cur_sp + 1, atom_only=False, depth=depth + 1)
                    if inner_ca is None or inner_ca[0] != close_sp - 1:
                        col_arith_memo[memo_key] = None
                        return None
                    atom_end = close_sp
                    atom_spans = inner_ca[1]
                    has_op = inner_ca[2] or (sign_count > 0 and bool(atom_spans))
                else:
                    num_a = _parse_num_expr_sp(cur_sp, atom_only=True, depth=depth + 1)
                    if num_a is None:
                        col_arith_memo[memo_key] = None
                        return None
                    atom_end = num_a[0]
                if atom_only or not _is_arith_op_sp(atom_end + 1):
                    res_ca = (atom_end, atom_spans, has_op)
                    col_arith_memo[memo_key] = res_ca
                    return res_ca
                spans_list: list[tuple[int, int, str]] = list(atom_spans)
                while _is_arith_op_sp(atom_end + 1):
                    rhs_ca = _parse_col_arith_expr_sp(atom_end + 2, atom_only=True, depth=depth + 1)
                    if rhs_ca is None:
                        col_arith_memo[memo_key] = None
                        return None
                    atom_end = rhs_ca[0]
                    if rhs_ca[1]:
                        spans_list.extend(rhs_ca[1])
                    has_op = True
                res_ca_f = (atom_end, tuple(spans_list), has_op)
                col_arith_memo[memo_key] = res_ca_f
                return res_ca_f

            def _valid_num_start_boundary(sp: int) -> bool:
                if sp <= 0:
                    return True
                pk, pt, _, _ = lex_toks[sig_c[sp - 1]]
                if (pk, pt) in (("OP", "("), ("OP", ","), ("OP", ";")):
                    return True
                return pk == "IDENT" and pt.upper() in _unary_kw

            def _valid_arith_start_boundary(sp: int) -> bool:
                if _valid_num_start_boundary(sp):
                    return True
                pk, pt, _, _ = lex_toks[sig_c[sp - 1]]
                return pk == "OP" and pt in ("=", "!=", "<>", "<", "<=", ">", ">=")

            def _valid_num_end_boundary(next_sp: int) -> bool:
                if next_sp >= len(sig_c):
                    return True
                nk, nt, _, _ = lex_toks[sig_c[next_sp]]
                if (nk, nt) in (("OP", ")"), ("OP", ","), ("OP", ";")):
                    return True
                return nk == "IDENT" and nt.upper() in _post_num_kw

            def _valid_arith_end_boundary(next_sp: int) -> bool:
                if _valid_num_end_boundary(next_sp):
                    return True
                nk, nt, _, _ = lex_toks[sig_c[next_sp]]
                if nk == "OP" and nt in ("=", "!=", "<>", "<", "<=", ">", ">="):
                    return True
                return nk == "IDENT" and nt.upper() in ("BETWEEN", "NOT", "IN", "GLOBAL", "IS")

            def _parse_numeric_in_list(lp_sp: int) -> int:
                if lp_sp >= len(sig_c) or lex_toks[sig_c[lp_sp]][0:2] != ("OP", "("):
                    return -1
                rp_sp = _match_paren_sp(lp_sp)
                if rp_sp <= lp_sp + 1:
                    return -1
                cur = lp_sp + 1
                item_count = 0
                while cur < rp_sp:
                    item_m = _parse_num_expr_sp(cur)
                    if item_m is None or item_m[0] >= rp_sp:
                        return -1
                    item_count += 1
                    nxt = item_m[0] + 1
                    if nxt == rp_sp:
                        return rp_sp
                    if lex_toks[sig_c[nxt]][0:2] != ("OP", ","):
                        return -1
                    cur = nxt + 1
                return -1

            from bisect import bisect_left as _bisect_left

            sorted_valid_col_starts = sorted(valid_col_starts)
            max_valid_col_sp = sorted_valid_col_starts[-1] if sorted_valid_col_starts else -1

            def _has_valid_col_in_range(lo_r: int, hi_r: int) -> bool:
                bi = _bisect_left(sorted_valid_col_starts, lo_r)
                return bi < len(sorted_valid_col_starts) and sorted_valid_col_starts[bi] <= hi_r

            edits: list[tuple[int, int, str]] = []
            sp_i = 0
            while sp_i < len(sig_c) and sp_i <= max_valid_col_sp:
                _sp_k, _sp_t, _, _ = lex_toks[sig_c[sp_i]]
                _is_col_cand = sp_i in valid_col_starts
                _is_num_cand = (
                    _sp_k == "NUM"
                    or (_sp_k == "OP" and _sp_t in ("(", "+", "-"))
                    or (_sp_k == "IDENT" and (_sp_t.upper() == "CAST" or sp_i in case_info_sp))
                )
                if not _is_col_cand and not _is_num_cand:
                    sp_i += 1
                    continue
                if _valid_arith_start_boundary(sp_i):
                    _try_arith = True
                    if (_sp_k, _sp_t) == ("OP", "("):
                        _cl_a = _match_paren_sp(sp_i)
                        if _cl_a > sp_i and not _has_valid_col_in_range(sp_i, _cl_a) and not _is_arith_op_sp(_cl_a + 1):
                            _try_arith = False
                    arith_m = _parse_col_arith_expr_sp(sp_i) if _try_arith else None
                    if arith_m is not None and arith_m[1] and arith_m[2] and _valid_arith_end_boundary(arith_m[0] + 1):
                        for a_start, a_end, a_core in arith_m[1]:
                            edits.append((a_start, a_end, f"toFloat64OrNull(nullIf({a_core}, ''))"))
                        sp_i = arith_m[0] + 1
                        continue
                col_m = _parse_str_col_sp(sp_i) if _is_col_cand else None
                if col_m is not None:
                    c_end, c_core, c_parens = col_m
                    op_sp = c_end + 1
                    if op_sp < len(sig_c) and lex_toks[sig_c[op_sp]][0] == "IDENT":
                        w1 = lex_toks[sig_c[op_sp]][1]
                        w1u = w1.upper()
                        # [GLOBAL] [NOT] IN (<num_expr>, ...)
                        in_lp_sp = -1
                        if w1u == "IN":
                            in_lp_sp = op_sp + 1
                        elif (
                            w1u in ("NOT", "GLOBAL")
                            and op_sp + 1 < len(sig_c)
                            and lex_toks[sig_c[op_sp + 1]][0] == "IDENT"
                        ):
                            w2u = lex_toks[sig_c[op_sp + 1]][1].upper()
                            if w2u == "IN":
                                in_lp_sp = op_sp + 2
                            elif (
                                ((w1u == "GLOBAL" and w2u == "NOT") or (w1u == "NOT" and w2u == "GLOBAL"))
                                and op_sp + 2 < len(sig_c)
                                and lex_toks[sig_c[op_sp + 2]][0] == "IDENT"
                                and lex_toks[sig_c[op_sp + 2]][1].upper() == "IN"
                            ):
                                in_lp_sp = op_sp + 3
                        if in_lp_sp > 0:
                            in_rp_sp = _parse_numeric_in_list(in_lp_sp)
                            if in_rp_sp > in_lp_sp and _valid_num_end_boundary(in_rp_sp + 1):
                                edits.append(
                                    (
                                        lex_toks[sig_c[sp_i]][2],
                                        lex_toks[sig_c[c_end]][3],
                                        f"toFloat64OrNull(nullIf({c_core}, ''))",
                                    )
                                )
                                sp_i = op_sp + 1
                                continue
                        bw_kw: str | None = None
                        low_sp = -1
                        if w1u == "BETWEEN":
                            bw_kw = w1
                            low_sp = op_sp + 1
                        elif (
                            w1u == "NOT"
                            and op_sp + 1 < len(sig_c)
                            and lex_toks[sig_c[op_sp + 1]][0] == "IDENT"
                            and lex_toks[sig_c[op_sp + 1]][1].upper() == "BETWEEN"
                        ):
                            bw_kw = f"{w1} {lex_toks[sig_c[op_sp + 1]][1]}"
                            low_sp = op_sp + 2
                        if bw_kw is not None:
                            low_m = _parse_num_expr_sp(low_sp)
                            if low_m is not None:
                                l_end, _, l_sub = low_m
                                and_sp = l_end + 1
                                if (
                                    and_sp < len(sig_c)
                                    and lex_toks[sig_c[and_sp]][0] == "IDENT"
                                    and lex_toks[sig_c[and_sp]][1].upper() == "AND"
                                ):
                                    high_m = _parse_num_expr_sp(and_sp + 1)
                                    if high_m is not None and _valid_num_end_boundary(high_m[0] + 1):
                                        h_end, _, h_sub = high_m
                                        and_kw = lex_toks[sig_c[and_sp]][1]
                                        if l_sub or h_sub:
                                            edits.append(
                                                (
                                                    lex_toks[sig_c[sp_i]][2],
                                                    lex_toks[sig_c[c_end]][3],
                                                    f"toFloat64OrNull(nullIf({c_core}, ''))",
                                                )
                                            )
                                            sp_i = op_sp + 1
                                        else:
                                            l_sql = seg_sql[lex_toks[sig_c[low_sp]][2] : lex_toks[sig_c[l_end]][3]]
                                            h_sql = seg_sql[lex_toks[sig_c[and_sp + 1]][2] : lex_toks[sig_c[h_end]][3]]
                                            edits.append(
                                                (
                                                    lex_toks[sig_c[sp_i]][2],
                                                    lex_toks[sig_c[h_end]][3],
                                                    f"if(isNull(toFloat64OrNull(nullIf({c_core}, ''))), NULL, toFloat64OrZero({c_core}) {bw_kw} {l_sql} {and_kw} {h_sql})",
                                                )
                                            )
                                            sp_i = h_end + 1
                                        continue
                    if (
                        op_sp < len(sig_c)
                        and lex_toks[sig_c[op_sp]][0] == "OP"
                        and lex_toks[sig_c[op_sp]][1] in ("=", "!=", "<>", "<", "<=", ">", ">=")
                    ):
                        op_txt = lex_toks[sig_c[op_sp]][1]
                        rhs_m = _parse_num_expr_sp(op_sp + 1)
                        if rhs_m is not None and _valid_num_end_boundary(rhs_m[0] + 1):
                            r_end, r_plain, r_sub = rhs_m
                            if r_sub:
                                edits.append(
                                    (
                                        lex_toks[sig_c[sp_i]][2],
                                        lex_toks[sig_c[c_end]][3],
                                        f"toFloat64OrNull(nullIf({c_core}, ''))",
                                    )
                                )
                                sp_i = op_sp + 1
                                continue
                            r_sql = seg_sql[lex_toks[sig_c[op_sp + 1]][2] : lex_toks[sig_c[r_end]][3]]
                            if op_txt in ("<", "<=", ">", ">="):
                                repl_s = f"if(isNull(toFloat64OrNull(nullIf({c_core}, ''))), NULL, toFloat64OrZero({c_core}) {op_txt} {r_sql})"
                            elif op_txt == "=" and r_plain and not c_parens:
                                repl_s = f"if(isNull(toFloat64OrNull(nullIf({c_core}, ''))), NULL, ({c_core} = '{r_sql}' OR toFloat64OrNull(nullIf({c_core}, '')) = {r_sql}))"
                            else:
                                repl_s = f"toFloat64OrNull(nullIf({c_core}, '')) {op_txt} {r_sql}"
                            edits.append((lex_toks[sig_c[sp_i]][2], lex_toks[sig_c[r_end]][3], repl_s))
                            sp_i = r_end + 1
                            continue
                if _valid_num_start_boundary(sp_i):
                    lhs_m = _parse_num_expr_sp(sp_i)
                    if lhs_m is not None:
                        l_end2, l_plain2, l_sub2 = lhs_m
                        op_sp2 = l_end2 + 1
                        if op_sp2 < len(sig_c) and lex_toks[sig_c[op_sp2]][0] == "IDENT":
                            rw1 = lex_toks[sig_c[op_sp2]][1].upper()
                            rbw_low_sp = -1
                            if rw1 == "BETWEEN":
                                rbw_low_sp = op_sp2 + 1
                            elif (
                                rw1 == "NOT"
                                and op_sp2 + 1 < len(sig_c)
                                and lex_toks[sig_c[op_sp2 + 1]][0] == "IDENT"
                                and lex_toks[sig_c[op_sp2 + 1]][1].upper() == "BETWEEN"
                            ):
                                rbw_low_sp = op_sp2 + 2
                            if rbw_low_sp > 0:
                                rlow_c = _parse_str_col_sp(rbw_low_sp)
                                rlow_n = None if rlow_c is not None else _parse_num_expr_sp(rbw_low_sp)
                                rlow_end = (
                                    rlow_c[0] if rlow_c is not None else (rlow_n[0] if rlow_n is not None else -1)
                                )
                                rand_sp = rlow_end + 1
                                if (
                                    rlow_end >= rbw_low_sp
                                    and rand_sp < len(sig_c)
                                    and lex_toks[sig_c[rand_sp]][0] == "IDENT"
                                    and lex_toks[sig_c[rand_sp]][1].upper() == "AND"
                                ):
                                    rhigh_c = _parse_str_col_sp(rand_sp + 1)
                                    rhigh_n = None if rhigh_c is not None else _parse_num_expr_sp(rand_sp + 1)
                                    rhigh_end = (
                                        rhigh_c[0]
                                        if rhigh_c is not None
                                        else (rhigh_n[0] if rhigh_n is not None else -1)
                                    )
                                    if (
                                        rhigh_end >= rand_sp + 1
                                        and (rlow_c is not None or rhigh_c is not None)
                                        and _valid_num_end_boundary(rhigh_end + 1)
                                    ):
                                        if rlow_c is not None:
                                            edits.append(
                                                (
                                                    lex_toks[sig_c[rbw_low_sp]][2],
                                                    lex_toks[sig_c[rlow_c[0]]][3],
                                                    f"toFloat64OrNull(nullIf({rlow_c[1]}, ''))",
                                                )
                                            )
                                        if rhigh_c is not None:
                                            edits.append(
                                                (
                                                    lex_toks[sig_c[rand_sp + 1]][2],
                                                    lex_toks[sig_c[rhigh_c[0]]][3],
                                                    f"toFloat64OrNull(nullIf({rhigh_c[1]}, ''))",
                                                )
                                            )
                                        sp_i = rhigh_end + 1
                                        continue
                        if (
                            op_sp2 < len(sig_c)
                            and lex_toks[sig_c[op_sp2]][0] == "OP"
                            and lex_toks[sig_c[op_sp2]][1] in ("=", "!=", "<>", "<", "<=", ">", ">=")
                        ):
                            op_txt2 = lex_toks[sig_c[op_sp2]][1]
                            rhs_c = _parse_str_col_sp(op_sp2 + 1)
                            if rhs_c is not None and _valid_num_end_boundary(rhs_c[0] + 1):
                                rc_end, rc_core, rc_parens = rhs_c
                                if l_sub2:
                                    edits.append(
                                        (
                                            lex_toks[sig_c[op_sp2 + 1]][2],
                                            lex_toks[sig_c[rc_end]][3],
                                            f"toFloat64OrNull(nullIf({rc_core}, ''))",
                                        )
                                    )
                                    sp_i = rc_end + 1
                                    continue
                                l_sql2 = seg_sql[lex_toks[sig_c[sp_i]][2] : lex_toks[sig_c[l_end2]][3]]
                                if op_txt2 in ("<", "<=", ">", ">="):
                                    repl_r = f"if(isNull(toFloat64OrNull(nullIf({rc_core}, ''))), NULL, {l_sql2} {op_txt2} toFloat64OrZero({rc_core}))"
                                elif op_txt2 == "=" and l_plain2 and not rc_parens:
                                    repl_r = f"if(isNull(toFloat64OrNull(nullIf({rc_core}, ''))), NULL, ('{l_sql2}' = {rc_core} OR {l_sql2} = toFloat64OrNull(nullIf({rc_core}, ''))))"
                                else:
                                    repl_r = f"{l_sql2} {op_txt2} toFloat64OrNull(nullIf({rc_core}, ''))"
                                edits.append((lex_toks[sig_c[sp_i]][2], lex_toks[sig_c[rc_end]][3], repl_r))
                                sp_i = rc_end + 1
                                continue
                        if not l_sub2 and l_end2 > sp_i:
                            sp_i = l_end2 + 1
                            continue
                sp_i += 1
            if edits:
                out_chunks: list[str] = []
                last_pos = 0
                for e_start, e_end, e_repl in edits:
                    out_chunks.append(seg_sql[last_pos:e_start])
                    out_chunks.append(e_repl)
                    last_pos = e_end
                out_chunks.append(seg_sql[last_pos:])
                seg_sql = "".join(out_chunks)

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

    sql_upper = sql.upper()
    has_any_setop_kw = "UNION" in sql_upper or "INTERSECT" in sql_upper or "EXCEPT" in sql_upper
    has_any_lparen = "(" in sql

    def _rewrite_tokens(
        tokens: list[tuple[str, str]],
        inherited_ctes: dict[str, tuple[set[str], _DerivedCfSchema]],
        *,
        is_subquery: bool = False,
        inherited_expr_ctes: dict[str, list[str]] | None = None,
    ) -> tuple[str, set[str], set[str], _DerivedCfSchema]:
        tokens, prefix_wrap_s, suffix_wrap_s = _peel_enclosing_parens(tokens)

        def _wrap(s: str) -> str:
            if not prefix_wrap_s and not suffix_wrap_s:
                return s
            return prefix_wrap_s + s + suffix_wrap_s

        ctes = dict(inherited_ctes)
        branch_expr_ctes: dict[str, list[str]] = dict(inherited_expr_ctes) if inherited_expr_ctes else {}
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

            _with_subq_heads = frozenset(
                {
                    "SELECT",
                    "WITH",
                    "FROM",
                    "TABLE",
                    "VALUES",
                    "SHOW",
                    "DESCRIBE",
                    "SUMMARIZE",
                    "DESC",
                    "PIVOT",
                    "UNPIVOT",
                    "PIVOT_WIDER",
                    "PIVOT_LONGER",
                }
            )
            _blocked_expr_cte_schemas = frozenset({"system", "information_schema", "pg_catalog"})
            _protected_tbl_names = frozenset(table_map.keys()) | frozenset(_FACT_DOMAINS.keys())
            expr_cte_table_refs = branch_expr_ctes
            with_sig_list, with_sig_pos, _, with_match_by_tok, _, _ = _get_lex_meta(tokens)

            def _parse_lex_expr_table_ref(expr_slice: list[tuple[str, str]]) -> list[str] | None:
                esig, _, _, _, ematch_s, _ = _get_lex_meta(expr_slice)
                if not esig:
                    return None
                lo_e, hi_e = 0, len(esig) - 1
                while lo_e <= hi_e:
                    if lo_e < hi_e and expr_slice[esig[lo_e]] == ("OP", "(") and ematch_s.get(lo_e) == hi_e:
                        lo_e += 1
                        hi_e -= 1
                        continue
                    cur_lo = expr_slice[esig[lo_e]]
                    if cur_lo == ("OP", "+") or (
                        cur_lo[0] == "OTHER" and bool(cur_lo[1]) and cur_lo[1].strip("+") == ""
                    ):
                        lo_e += 1
                        continue
                    if (
                        hi_e - lo_e >= 2
                        and expr_slice[esig[hi_e - 1]][0] == "IDENT"
                        and expr_slice[esig[hi_e - 1]][1].upper() == "AS"
                        and expr_slice[esig[hi_e]][0] in ("IDENT", "DQUOTE")
                    ):
                        hi_e -= 2
                        continue
                    break
                span_e = hi_e - lo_e + 1
                if span_e <= 0 or span_e % 2 == 0:
                    return None
                parts_e: list[str] = []
                for off_e in range(span_e):
                    ek, et = expr_slice[esig[lo_e + off_e]]
                    if off_e % 2 == 0:
                        if ek not in ("IDENT", "DQUOTE"):
                            return None
                        parts_e.append(_unquote_sql_ident(et))
                    elif (ek, et) != ("OP", "."):
                        return None
                if (
                    len(parts_e) == 1
                    and expr_slice[esig[lo_e]][0] == "IDENT"
                    and parts_e[0].upper()
                    in (
                        "TRUE",
                        "FALSE",
                        "NULL",
                    )
                ):
                    return None
                return parts_e

            def _find_lex_expr_cte_as(start_tok_idx: int) -> tuple[int, int] | None:
                k_e = start_tok_idx
                n_t = len(tokens)
                while k_e < n_t:
                    ek, et = tokens[k_e]
                    if ek in ("WS", "COMMENT"):
                        k_e += 1
                        continue
                    if ek == "OP" and et in ("(", "[", "{"):
                        cl_e = with_match_by_tok.get(k_e, -1)
                        if cl_e <= k_e:
                            return None
                        k_e = cl_e + 1
                        continue
                    if ek == "OP" and et in (",", ";"):
                        return None
                    if ek == "IDENT":
                        eu = et.upper()
                        if eu == "AS" and k_e > start_tok_idx:
                            prev_e = k_e - 1
                            while prev_e >= start_tok_idx and tokens[prev_e][0] in ("WS", "COMMENT"):
                                prev_e -= 1
                            if prev_e >= start_tok_idx and not _is_dot_or_dcolon_lex(tokens, prev_e):
                                al_e = k_e + 1
                                while al_e < n_t and tokens[al_e][0] in ("WS", "COMMENT"):
                                    al_e += 1
                                if al_e < n_t and tokens[al_e][0] in ("IDENT", "DQUOTE"):
                                    aft_e = al_e + 1
                                    while aft_e < n_t and tokens[aft_e][0] in ("WS", "COMMENT"):
                                        aft_e += 1
                                    if aft_e < n_t:
                                        ak_e, at_e = tokens[aft_e]
                                        if (ak_e, at_e) == ("OP", ","):
                                            return k_e, al_e
                                        if ak_e == "IDENT" and at_e.upper() in _with_subq_heads:
                                            return k_e, al_e
                                        if (ak_e, at_e) == ("OP", "("):
                                            cl_sub = with_match_by_tok.get(aft_e, -1)
                                            if cl_sub > aft_e and _is_subquery_tokens(tokens[aft_e : cl_sub + 1]):
                                                return k_e, al_e
                        elif (
                            eu == "SELECT"
                            and not _is_non_kw_lex_pos(tokens, k_e)
                            and not _is_lex_dot_qualifier(tokens, with_sig_pos.get(k_e, -1), with_sig_list)
                            and (
                                k_e == start_tok_idx
                                or (
                                    (s_ke := with_sig_pos.get(k_e, -1)) <= 0
                                    or not _is_lex_on_operator_tok(tokens, s_ke - 1, with_sig_list)
                                )
                            )
                        ):
                            return None
                    k_e += 1
                return None

            parsed_any_cte = False
            expecting_cte_item = True
            while i_tok < len(tokens):
                p = i_tok
                while p < len(tokens) and tokens[p][0] in ("WS", "COMMENT"):
                    p += 1
                if p >= len(tokens):
                    break
                matched_std = False
                if tokens[p][0] in ("IDENT", "DQUOTE") and not (
                    tokens[p][0] == "IDENT" and tokens[p][1].upper() == "SELECT"
                ):
                    cte_name = _unquote_sql_ident(tokens[p][1])
                    p2 = p + 1
                    while p2 < len(tokens) and tokens[p2][0] in ("WS", "COMMENT"):
                        p2 += 1
                    explicit_cte_cols: list[str] | None = None
                    cols_valid = True
                    if p2 < len(tokens) and tokens[p2] == ("OP", "("):
                        rp_cols = _find_matching_paren(tokens, p2)
                        if not _is_valid_lex_ident_list(tokens[p2 + 1 : rp_cols]):
                            cols_valid = False
                        else:
                            explicit_cte_cols = [
                                _unquote_sql_ident(tk).lower()
                                for kd, tk in tokens[p2 + 1 : rp_cols]
                                if kd in ("IDENT", "DQUOTE")
                            ]
                            p2 = rp_cols + 1
                            while p2 < len(tokens) and tokens[p2][0] in ("WS", "COMMENT"):
                                p2 += 1
                    if (
                        cols_valid
                        and p2 < len(tokens)
                        and tokens[p2][0] == "IDENT"
                        and tokens[p2][1].upper() == "AS"
                        and not _is_non_kw_lex_pos(tokens, p2)
                    ):
                        p2 += 1
                        while p2 < len(tokens):
                            if tokens[p2][0] in ("WS", "COMMENT"):
                                p2 += 1
                            elif tokens[p2][0] == "IDENT" and tokens[p2][1].upper() in ("NOT", "MATERIALIZED"):
                                p2 += 1
                            else:
                                break
                        if p2 < len(tokens) and tokens[p2] == ("OP", "("):
                            close_idx = _find_matching_paren(tokens, p2)
                            if _is_subquery_tokens(tokens[p2 + 1 : close_idx]):
                                inner_sql, inner_domains, inner_canonicals, inner_schema = _rewrite_tokens(
                                    tokens[p2 + 1 : close_idx],
                                    ctes,
                                    is_subquery=True,
                                    inherited_expr_ctes=expr_cte_table_refs,
                                )
                                if explicit_cte_cols is not None:
                                    inner_domains = _demote_col_list_domains(inner_domains)
                                    inner_schema = _remap_derived_cf_schema(inner_schema, explicit_cte_cols)
                                ctes[cte_name.lower()] = (inner_domains, inner_schema)
                                expr_cte_table_refs.pop(cte_name.lower(), None)
                                all_canonicals |= inner_canonicals
                                buf.extend(tk for _, tk in tokens[i_tok : p2 + 1])
                                buf.append(inner_sql)
                                buf.append(")")
                                i_tok = close_idx + 1
                                parsed_any_cte = True
                                matched_std = True

                if matched_std:
                    next_sig = i_tok
                    while next_sig < len(tokens) and tokens[next_sig][0] in ("WS", "COMMENT"):
                        next_sig += 1
                    if next_sig < len(tokens) and tokens[next_sig] == ("OP", ","):
                        buf.extend(tk for _, tk in tokens[i_tok : next_sig + 1])
                        i_tok = next_sig + 1
                        expecting_cte_item = True
                        continue
                    expecting_cte_item = False
                    break

                expr_hit = _find_lex_expr_cte_as(p)
                if expr_hit is not None:
                    as_idx_e, alias_idx_e = expr_hit
                    expr_slice = tokens[p:as_idx_e]
                    alias_low_e = _unquote_sql_ident(tokens[alias_idx_e][1]).lower()
                    ref_parts = _parse_lex_expr_table_ref(expr_slice)
                    if ref_parts is not None:
                        if len(ref_parts) == 1 and ref_parts[0].lower() in expr_cte_table_refs:
                            ref_parts = expr_cte_table_refs[ref_parts[0].lower()]
                        if len(ref_parts) > 3:
                            raise PermissionError("invalid table reference in WITH expression")
                        tbl_part_low = ref_parts[-1].strip().lower()
                        if "/" in tbl_part_low or "\\" in tbl_part_low or "." in tbl_part_low:
                            raise PermissionError(
                                f"table name {ref_parts[-1]!r} contains path-like characters in console queries"
                            )
                        if tbl_part_low.startswith("pg_"):
                            raise PermissionError(
                                f"table {ref_parts[-1]!r} uses restricted prefix 'pg_' in console queries"
                            )
                        if len(ref_parts) >= 2 and ref_parts[-2].strip().lower() in _blocked_expr_cte_schemas:
                            raise PermissionError(
                                f"restricted schema {ref_parts[-2]!r} is not allowed in console queries"
                            )
                        if len(ref_parts) == 3 and ref_parts[0].strip().lower() != "main":
                            raise PermissionError(
                                f"restricted catalog {ref_parts[0]!r} is not allowed in console queries"
                            )
                        if alias_low_e in _protected_tbl_names:
                            raise PermissionError(
                                f"WITH expression alias {alias_low_e!r} cannot shadow a table in console queries"
                            )
                        expr_cte_table_refs[alias_low_e] = ref_parts
                    else:
                        expr_cte_table_refs.pop(alias_low_e, None)
                    if _has_enclosing_parens(expr_slice) is not None and _is_subquery_tokens(expr_slice):
                        sub_e_sql, _, sub_e_canons, _ = _rewrite_tokens(
                            expr_slice, ctes, is_subquery=True, inherited_expr_ctes=expr_cte_table_refs
                        )
                    else:
                        sub_e_sql, _, sub_e_canons, _ = _rewrite_single_branch(expr_slice, ctes, is_subquery=True)
                    all_canonicals |= sub_e_canons
                    buf.extend(tk for _, tk in tokens[i_tok:p])
                    buf.append(sub_e_sql)
                    buf.extend(tk for _, tk in tokens[as_idx_e : alias_idx_e + 1])
                    i_tok = alias_idx_e + 1
                    parsed_any_cte = True
                    next_sig = i_tok
                    while next_sig < len(tokens) and tokens[next_sig][0] in ("WS", "COMMENT"):
                        next_sig += 1
                    if next_sig < len(tokens) and tokens[next_sig] == ("OP", ","):
                        buf.extend(tk for _, tk in tokens[i_tok : next_sig + 1])
                        i_tok = next_sig + 1
                        expecting_cte_item = True
                        continue
                    expecting_cte_item = False
                    break
                raise PermissionError("SQL parse error: invalid WITH clause item")

            if expecting_cte_item:
                raise PermissionError("SQL parse error: incomplete WITH clause")

            if parsed_any_cte:
                out_prefix = "".join(buf)
                tokens = tokens[i_tok:]
                if not tokens:
                    return _wrap(out_prefix), set(), all_canonicals, _DerivedCfSchema()

        if not has_any_setop_kw:
            b_sql, b_doms, b_canons, b_sch = _rewrite_single_branch(tokens, ctes, is_subquery=is_subquery)
            return _wrap(out_prefix + b_sql), b_doms, all_canonicals | b_canons, b_sch

        top_sig, top_sig_pos, _, _, _, _ = _get_lex_meta(tokens)
        branches: list[tuple[list[tuple[str, str]], list[tuple[str, str]]]] = []
        cur_branch: list[tuple[str, str]] = []
        i_tok = 0
        depth = 0
        split_in_from = False
        split_expect_table = False
        split_in_on = False
        split_in_array_join = False
        while i_tok < len(tokens):
            k, t = tokens[i_tok]
            if k == "OP" and t in ("(", "[", "{"):
                if depth == 0 and split_in_from and split_expect_table:
                    split_expect_table = False
                depth += 1
            elif k == "OP" and t in (")", "]", "}"):
                depth = max(0, depth - 1)
            elif depth == 0 and k == "OP" and t == "," and split_in_from:
                split_expect_table = not split_in_array_join
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
                    if (
                        wu_top in ("UNION", "INTERSECT", "EXCEPT")
                        and not _is_lex_star_except(tokens, s_pos_top, top_sig)
                        and _is_lex_multi_token_post_on_exit(tokens, s_pos_top, top_sig)
                    ):
                        sep: list[tuple[str, str]] = [tokens[i_tok]]
                        j = i_tok + 1
                        while j < len(tokens):
                            if tokens[j][0] in ("WS", "COMMENT"):
                                sep.append(tokens[j])
                                j += 1
                            elif tokens[j][0] == "IDENT" and tokens[j][1].upper() in ("ALL", "DISTINCT"):
                                sep.append(tokens[j])
                                j += 1
                            elif tokens[j][0] == "IDENT" and tokens[j][1].upper() == "BY":
                                j_n = j + 1
                                while j_n < len(tokens) and tokens[j_n][0] in ("WS", "COMMENT"):
                                    j_n += 1
                                if j_n < len(tokens) and tokens[j_n][0] == "IDENT" and tokens[j_n][1].upper() == "NAME":
                                    sep.extend(tokens[j : j_n + 1])
                                    j = j_n + 1
                                else:
                                    break
                            else:
                                break
                        branches.append((cur_branch, sep))
                        cur_branch = []
                        split_in_from = False
                        split_expect_table = False
                        split_in_on = False
                        split_in_array_join = False
                        i_tok = j
                        continue
                    if (wu_top == "FROM" and not _is_lex_distinct_from(tokens, s_pos_top, top_sig)) or wu_top == "JOIN":
                        is_arr_join_tok = (
                            wu_top == "JOIN"
                            and s_pos_top > 0
                            and tokens[top_sig[s_pos_top - 1]][0] == "IDENT"
                            and tokens[top_sig[s_pos_top - 1]][1].upper() == "ARRAY"
                            and not _is_non_kw_lex_pos(tokens, top_sig[s_pos_top - 1])
                        )
                        split_in_from = True
                        split_in_array_join = is_arr_join_tok
                        split_expect_table = not is_arr_join_tok
                        split_in_on = False
                    elif wu_top in _JOIN_MODIFIER_KEYWORDS:
                        if split_in_from:
                            split_expect_table = False
                            split_in_array_join = False
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
                            split_in_array_join = False
                    elif (
                        split_in_from
                        and not split_expect_table
                        and not split_in_on
                        and not split_in_array_join
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
                        is_lparen_top = False
                        if after_top == ("OP", "("):
                            if wu_top == "PREWHERE":
                                lp_pw_t = top_sig[s_pos_top + 1]
                                rp_pw_t = _find_matching_paren(tokens, lp_pw_t)
                                rp_pw_s = top_sig_pos.get(rp_pw_t, -1)
                                after_pw_t = (
                                    tokens[top_sig[rp_pw_s + 1]]
                                    if (rp_pw_s >= 0 and rp_pw_s + 1 < len(top_sig))
                                    else None
                                )
                                is_lparen_top = (
                                    rp_pw_t > lp_pw_t
                                    and _is_valid_lex_ident_list(tokens[lp_pw_t + 1 : rp_pw_t])
                                    and after_pw_t is not None
                                    and (
                                        after_pw_t == ("OP", ",")
                                        or (
                                            after_pw_t[0] == "IDENT"
                                            and after_pw_t[1].upper() in (_JOIN_MODIFIER_KEYWORDS | {"JOIN", "FINAL"})
                                        )
                                    )
                                )
                            else:
                                is_lparen_top = True
                        is_bare_top = (
                            after_top is None
                            or after_top == ("OP", ",")
                            or is_lparen_top
                            or (after_top[0] == "IDENT" and after_top[1].upper() in _SQL_CLAUSE_STOPWORDS)
                        )
                        if not is_bare_top and wu_top != "SAMPLE":
                            split_in_from = False
                            split_in_on = False
                            split_in_array_join = False
                    elif (
                        (wu_top in _FROM_EXIT_KEYWORDS and not _is_lex_star_except(tokens, s_pos_top, top_sig))
                        or (split_in_from and wu_top == "SELECT")
                    ) and _can_exit_from_lex(tokens, s_pos_top, top_sig, in_on_clause=split_in_on):
                        split_in_from = False
                        split_expect_table = False
                        split_in_on = False
                        split_in_array_join = False
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
            b_sig = _sig_indices(b_tokens)
            starts_with_cte = bool(
                len(branches) > 1
                and b_sig
                and b_tokens[b_sig[0]][0] == "IDENT"
                and b_tokens[b_sig[0]][1].upper() == "WITH"
            )
            if _has_enclosing_parens(b_tokens) is not None or starts_with_cte:
                b_sql, b_domains, b_canonicals, b_schema = _rewrite_tokens(
                    b_tokens, ctes, is_subquery=is_subquery, inherited_expr_ctes=branch_expr_ctes
                )
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
        processed: list[tuple[str, str]]
        subquery_placeholders: dict[str, str] = {}
        subquery_domains: dict[str, set[str]] = {}
        subquery_schema: dict[str, _DerivedCfSchema] = {}
        sub_nonce = _secrets.token_hex(6)

        if not has_any_lparen:
            processed = tokens
        else:
            tok_sig, tok_sig_pos, _, tok_match_by_tok, tok_match_by_s, _ = _get_lex_meta(tokens)
            n_tok_sig = len(tok_sig)
            not_subq_open_s: set[int] = set()
            processed = []
            i = 0
            while i < len(tokens):
                k, t = tokens[i]
                if k == "OP" and t == "(":
                    open_s = tok_sig_pos[i]
                    if open_s not in not_subq_open_s:
                        raw_cl = tok_match_by_tok.get(i, -1)
                        close_idx = raw_cl if raw_cl > i else len(tokens) - 1
                        close_s = tok_match_by_s.get(open_s, -1)
                        lo_s = open_s + 1
                        hi_s = (close_s - 1) if close_s > open_s else (n_tok_sig - 1)
                        prev_fn_upper = (
                            tokens[tok_sig[open_s - 1]][1].upper()
                            if open_s > 0 and tokens[tok_sig[open_s - 1]][0] == "IDENT"
                            else ""
                        )
                        is_trim_from = (
                            prev_fn_upper in ("TRIM", "BTRIM", "LTRIM", "RTRIM")
                            and lo_s <= hi_s
                            and tokens[tok_sig[lo_s]][0] == "IDENT"
                            and tokens[tok_sig[lo_s]][1].upper() == "FROM"
                        )
                        is_subq_span = False
                        peeled_opens: list[int] = [open_s]
                        if not is_trim_from and lo_s <= hi_s:
                            cur_lo, cur_hi = lo_s, hi_s
                            while cur_lo <= cur_hi:
                                while (
                                    cur_lo < cur_hi
                                    and tokens[tok_sig[cur_lo]] == ("OP", "(")
                                    and tok_match_by_s.get(cur_lo) == cur_hi
                                ):
                                    peeled_opens.append(cur_lo)
                                    cur_lo += 1
                                    cur_hi -= 1
                                k0_s, t0_s = tokens[tok_sig[cur_lo]]
                                if k0_s == "IDENT" and t0_s.upper() in ("SELECT", "WITH", "FROM", "TABLE"):
                                    is_subq_span = True
                                    break
                                if k0_s == "OP" and t0_s == "(":
                                    cl_first_s = tok_match_by_s.get(cur_lo, cur_hi)
                                    if cl_first_s < cur_hi:
                                        nk_s, nt_s = tokens[tok_sig[cl_first_s + 1]]
                                        if nk_s == "IDENT" and nt_s.upper() in (
                                            "UNION",
                                            "INTERSECT",
                                            "EXCEPT",
                                            "ORDER",
                                            "LIMIT",
                                            "OFFSET",
                                            "FETCH",
                                        ):
                                            cur_lo = cur_lo + 1
                                            cur_hi = cl_first_s - 1
                                            continue
                                break
                        if is_subq_span:
                            inner = tokens[i + 1 : close_idx]
                            sub_sql, sub_doms, sub_canons, sub_sch = _rewrite_tokens(inner, ctes, is_subquery=True)
                            branch_canonicals |= sub_canons
                            ph = f"__fla_subq_{sub_nonce}_{len(subquery_placeholders)}__"
                            subquery_placeholders[ph] = f"({sub_sql})"
                            subquery_domains[ph] = sub_doms
                            subquery_schema[ph] = sub_sch
                            processed.append(("SUBQ", ph))
                            i = close_idx + 1
                            continue
                        not_subq_open_s.update(peeled_opens)
                processed.append((k, t))
                i += 1
            if not subquery_placeholders:
                processed = tokens

        sig_idxs, proc_sig_pos, proc_as_run, _, proc_match_by_s, _ = _get_lex_meta(processed)

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
            if after == ("OP", "("):
                if cu == "PREWHERE":
                    lp_pw_b = sig_idxs[cand_s_pos + 1]
                    rp_pw_b = _find_matching_paren(processed, lp_pw_b)
                    rp_pw_bs = proc_sig_pos.get(rp_pw_b, -1)
                    after_pw_b = (
                        processed[sig_idxs[rp_pw_bs + 1]] if (rp_pw_bs >= 0 and rp_pw_bs + 1 < len(sig_idxs)) else None
                    )
                    return (
                        rp_pw_b > lp_pw_b
                        and _is_valid_lex_ident_list(processed[lp_pw_b + 1 : rp_pw_b])
                        and after_pw_b is not None
                        and (
                            after_pw_b == ("OP", ",")
                            or (
                                after_pw_b[0] == "IDENT"
                                and after_pw_b[1].upper() in (_JOIN_MODIFIER_KEYWORDS | {"JOIN", "FINAL"})
                            )
                        )
                    )
                return True
            return (
                after is None
                or after == ("OP", ",")
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

        _pos_in_disallowed_kw = frozenset(
            {"AND", "OR", "XOR", "NOT", "IS", "LIKE", "ILIKE", "RLIKE", "REGEXP", "GLOB", "SIMILAR", "BETWEEN"}
        )
        pos_in_frame_cache: dict[int, int | None] = {}

        def _is_valid_lex_position_in(open_s_pos: int, in_s_pos: int) -> bool:
            if open_s_pos in pos_in_frame_cache:
                return pos_in_frame_cache[open_s_pos] == in_s_pos
            close_s_pos = proc_match_by_s.get(open_s_pos, -1)
            if close_s_pos <= open_s_pos:
                pos_in_frame_cache[open_s_pos] = None
                return False
            case_depth = 0
            top_in_count = 0
            valid_s: int | None = None
            cur_s = open_s_pos + 1
            while cur_s < close_s_pos:
                ck_p, ct_p = processed[sig_idxs[cur_s]]
                if ck_p == "OP" and ct_p in ("(", "[", "{"):
                    cl_s = proc_match_by_s.get(cur_s, -1)
                    if cl_s <= cur_s:
                        valid_s = None
                        top_in_count = 0
                        break
                    cur_s = cl_s + 1
                    continue
                if (ck_p, ct_p) == ("OP", ","):
                    valid_s = None
                    top_in_count = 0
                    break
                if case_depth == 0 and (
                    (ck_p == "OP" and ct_p in ("=", "<", ">", "<=", ">=", "!=", "<>"))
                    or (ck_p == "OTHER" and any(ch in ct_p for ch in ("=", "<", ">", "!")))
                ):
                    valid_s = None
                    top_in_count = 0
                    break
                if (
                    ck_p == "IDENT"
                    and not _is_non_kw_lex_pos(processed, sig_idxs[cur_s])
                    and not _is_lex_dot_qualifier(processed, cur_s, sig_idxs)
                ):
                    cwu = ct_p.upper()
                    if cwu == "CASE":
                        case_depth += 1
                    elif cwu == "END":
                        case_depth = max(0, case_depth - 1)
                    elif cwu == "IN":
                        top_in_count += 1
                        if top_in_count > 1:
                            valid_s = None
                            break
                        if case_depth == 0 and open_s_pos + 1 < cur_s < close_s_pos - 1:
                            prev_k_p, prev_t_p = processed[sig_idxs[cur_s - 1]]
                            if not (prev_k_p == "IDENT" and prev_t_p.upper() in ("NOT", "GLOBAL")):
                                valid_s = cur_s
                            else:
                                valid_s = None
                                break
                        else:
                            valid_s = None
                            break
                    elif case_depth == 0 and cwu in _pos_in_disallowed_kw:
                        valid_s = None
                        top_in_count = 0
                        break
                cur_s += 1
            res_s = valid_s if top_in_count == 1 else None
            pos_in_frame_cache[open_s_pos] = res_s
            return res_s == in_s_pos

        def _is_lex_prefix_in_call(in_s_pos: int) -> bool:
            p = in_s_pos - 1
            if (
                p >= 0
                and processed[sig_idxs[p]][0] == "IDENT"
                and processed[sig_idxs[p]][1].upper() == "NOT"
                and not _is_non_kw_lex_pos(processed, sig_idxs[p])
            ):
                p -= 1
            if (
                p >= 0
                and processed[sig_idxs[p]][0] == "IDENT"
                and processed[sig_idxs[p]][1].upper() == "GLOBAL"
                and not _is_non_kw_lex_pos(processed, sig_idxs[p])
            ):
                p -= 1
            if p < 0:
                return True
            if _is_lex_on_operator_tok(processed, p, sig_idxs):
                return True
            pk, pt = processed[sig_idxs[p]]
            return (
                pk == "IDENT"
                and pt.upper() in _IN_PREFIX_PREV_KEYWORDS
                and not _is_non_kw_lex_pos(processed, sig_idxs[p])
            )

        def _is_lex_in_rhs_table_ident(open_s_pos: int) -> bool:
            close_s_pos = proc_match_by_s.get(open_s_pos, -1)
            if close_s_pos <= open_s_pos:
                return False
            lo, hi = open_s_pos + 1, close_s_pos - 1
            while lo <= hi:
                if lo < hi and processed[sig_idxs[lo]] == ("OP", "(") and proc_match_by_s.get(lo) == hi:
                    lo += 1
                    hi -= 1
                    continue
                cur_in_lo = processed[sig_idxs[lo]]
                if cur_in_lo == ("OP", "+") or (
                    cur_in_lo[0] == "OTHER" and bool(cur_in_lo[1]) and cur_in_lo[1].strip("+") == ""
                ):
                    lo += 1
                    continue
                if (
                    hi - lo >= 2
                    and processed[sig_idxs[hi - 1]][0] == "IDENT"
                    and processed[sig_idxs[hi - 1]][1].upper() == "AS"
                    and processed[sig_idxs[hi]][0] in ("IDENT", "DQUOTE")
                ):
                    hi -= 2
                    continue
                break
            span_len = hi - lo + 1
            if span_len <= 0 or span_len % 2 == 0:
                return False
            for off in range(span_len):
                rk, rt = processed[sig_idxs[lo + off]]
                if off % 2 == 0:
                    if rk not in ("IDENT", "DQUOTE"):
                        return False
                elif (rk, rt) != ("OP", "."):
                    return False
            if span_len == 1:
                rk0, rt0 = processed[sig_idxs[lo]]
                if rk0 == "IDENT" and rt0.upper() in ("TRUE", "FALSE", "NULL"):
                    return False
            return True

        bjm_cache_start = -1
        bjm_cache_end = -1
        bjm_cache_ok = False

        def _starts_branch_join_mod_seq(start_sp: int) -> bool:
            nonlocal bjm_cache_start, bjm_cache_end, bjm_cache_ok
            if bjm_cache_start <= start_sp < bjm_cache_end:
                return bjm_cache_ok
            q_s = start_sp
            while (
                q_s < len(sig_idxs)
                and processed[sig_idxs[q_s]][0] == "IDENT"
                and processed[sig_idxs[q_s]][1].upper() in _JOIN_MODIFIER_KEYWORDS
            ):
                q_s += 1
            ok = False
            if q_s < len(sig_idxs) and processed[sig_idxs[q_s]][0] == "IDENT":
                qu = processed[sig_idxs[q_s]][1].upper()
                if qu == "JOIN":
                    ok = True
                elif (
                    qu == "ARRAY"
                    and q_s + 1 < len(sig_idxs)
                    and processed[sig_idxs[q_s + 1]][0] == "IDENT"
                    and processed[sig_idxs[q_s + 1]][1].upper() == "JOIN"
                ):
                    ok = True
            bjm_cache_start = start_sp
            bjm_cache_end = q_s
            bjm_cache_ok = ok
            return ok

        table_token_idxs: dict[int, str] = {}
        col_list_table_spans: dict[int, tuple[int, str]] = {}
        table_alias_idxs: set[int] = set()
        from_source_schemas: list[_DerivedCfSchema] = []
        func_stack: list[tuple[str, int]] = []
        top_from_s_pos: int | None = None
        in_from = False
        expect_table = False
        in_sample_clause = False
        in_on_clause = False
        in_array_join = False
        depth = 0
        s_pos = 0
        n_sig_b = len(sig_idxs)
        outside_from_kws = frozenset({"IN", "FROM", "JOIN", "EXCEPT"})
        while s_pos < n_sig_b:
            idx = sig_idxs[s_pos]
            k, t = processed[idx]
            if depth == 0 and not in_from:
                if k == "IDENT":
                    if len(t) < 2 or t.upper() not in outside_from_kws:
                        s_pos += 1
                        continue
                elif not (k == "OP" and t in ("(", "[", "{", ")", "]", "}")):
                    s_pos += 1
                    continue
            if k == "OP" and t in ("(", "[", "{"):
                if depth == 0 and in_from and expect_table:
                    raise PermissionError(
                        "parenthesized non-subquery table expressions are not allowed in console queries"
                    )
                if t == "(":
                    prev_fn = processed[sig_idxs[s_pos - 1]] if s_pos > 0 else None
                    func_stack.append((prev_fn[1].upper() if prev_fn and prev_fn[0] == "IDENT" else "", s_pos))
                else:
                    func_stack.append((t, s_pos))
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
                is_valid_pos_in = (
                    bool(func_stack)
                    and func_stack[-1][0] == "POSITION"
                    and _is_valid_lex_position_in(func_stack[-1][1], s_pos)
                )
                if not is_valid_pos_in:
                    nxt_tok = processed[sig_idxs[s_pos + 1]] if s_pos + 1 < len(sig_idxs) else None
                    if nxt_tok == ("OP", "(") and _is_lex_prefix_in_call(s_pos):
                        raise PermissionError(f"function {t!r} is not allowed in console queries")
                    if nxt_tok is None or (nxt_tok[0] != "SUBQ" and nxt_tok != ("OP", "(")):
                        raise PermissionError(
                            "IN must be followed by a parenthesized list or subquery in console queries"
                        )
                    if nxt_tok == ("OP", "(") and _is_lex_in_rhs_table_ident(s_pos + 1):
                        raise PermissionError(
                            "table references in IN (...) must use a SELECT subquery in console queries"
                        )

            if depth > 0:
                s_pos += 1
                continue

            is_non_kw_cur = False
            if k == "IDENT" and s_pos > 0:
                prev_p = processed[sig_idxs[s_pos - 1]]
                if prev_p == ("OP", "."):
                    is_non_kw_cur = True
                elif proc_as_run[s_pos] > 0 or prev_p == ("OTHER", ":"):
                    is_non_kw_cur = _is_non_kw_lex_pos(processed, idx)

            if k == "IDENT" and not is_non_kw_cur:
                wu = t.upper()
                if (
                    _is_lex_dot_qualifier(processed, s_pos, sig_idxs)
                    or (
                        in_on_clause
                        and not expect_table
                        and (wu in _FROM_EXIT_KEYWORDS or wu in ("SELECT", "FROM", "JOIN", "TABLE"))
                        and _is_lex_on_expr_ident(processed, s_pos, sig_idxs)
                    )
                    or (wu == "GROUP" and _is_lex_within_group(processed, s_pos, sig_idxs))
                ):
                    in_sample_clause = False
                    s_pos += 1
                    continue
                if wu == "EXCEPT" and _is_lex_star_except(processed, s_pos, sig_idxs):
                    lp_ex = sig_idxs[s_pos + 1]
                    rp_ex = _find_matching_paren(processed, lp_ex)
                    for ex_tok_i in range(lp_ex + 1, rp_ex):
                        if processed[ex_tok_i][0] in ("IDENT", "DQUOTE"):
                            table_alias_idxs.add(ex_tok_i)
                    s_pos += 1
                    continue
                if (
                    in_from
                    and not expect_table
                    and not in_sample_clause
                    and not (in_on_clause and _is_lex_on_expr_ident(processed, s_pos, sig_idxs))
                    and wu == "ARRAY"
                    and s_pos + 1 < len(sig_idxs)
                    and processed[sig_idxs[s_pos + 1]][0] == "IDENT"
                    and processed[sig_idxs[s_pos + 1]][1].upper() == "JOIN"
                ):
                    in_array_join = True
                    expect_table = False
                    in_sample_clause = False
                    in_on_clause = False
                    s_pos += 2
                    continue
                if (wu == "FROM" and not _is_lex_distinct_from(processed, s_pos, sig_idxs)) or wu == "JOIN":
                    if wu == "FROM" and top_from_s_pos is None:
                        top_from_s_pos = s_pos
                    in_from = True
                    in_array_join = False
                    expect_table = True
                    in_sample_clause = False
                    in_on_clause = False
                    s_pos += 1
                    continue
                if wu in _JOIN_MODIFIER_KEYWORDS:
                    if in_from and _starts_branch_join_mod_seq(s_pos):
                        expect_table = False
                        in_sample_clause = False
                        in_on_clause = False
                        in_array_join = False
                    elif in_from and not (
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
                if (
                    in_from
                    and not expect_table
                    and not in_on_clause
                    and not in_array_join
                    and _is_bare_clause_alias_at(s_pos)
                ):
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
                    in_array_join = False
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
                if in_from and not in_on_clause and not in_array_join and wu in ("FINAL", "SAMPLE"):
                    expect_table = False
                    in_sample_clause = wu == "SAMPLE"
                    s_pos += 1
                    continue

            if in_from and k == "OP" and t == ",":
                expect_table = not in_array_join
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
            if not _has_any_delim_sql and "," not in sql:
                if sel_start < sel_end:
                    proj_items.append(sig_idxs[sel_start:sel_end])
            else:
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
                is_bare_star_item = (len(it_idxs) == 1 and processed[it_idxs[0]] == ("OP", "*")) or (
                    len(it_idxs) >= 4
                    and processed[it_idxs[0]] == ("OP", "*")
                    and processed[it_idxs[1]][0] == "IDENT"
                    and processed[it_idxs[1]][1].upper() == "EXCEPT"
                    and processed[it_idxs[2]] == ("OP", "(")
                )
                is_qual_star_item = (
                    len(it_idxs) == 3
                    and processed[it_idxs[0]][0] in ("IDENT", "DQUOTE")
                    and processed[it_idxs[1]] == ("OP", ".")
                    and processed[it_idxs[2]] == ("OP", "*")
                ) or (
                    len(it_idxs) >= 6
                    and processed[it_idxs[0]][0] in ("IDENT", "DQUOTE")
                    and processed[it_idxs[1]] == ("OP", ".")
                    and processed[it_idxs[2]] == ("OP", "*")
                    and processed[it_idxs[3]][0] == "IDENT"
                    and processed[it_idxs[3]][1].upper() == "EXCEPT"
                    and processed[it_idxs[4]] == ("OP", "(")
                )
                if is_bare_star_item:
                    projects_star = True
                    out_str_cf_cols |= branch_inherited_str_cf
                    out_bool_cf_cols |= branch_inherited_bool_cf
                    out_all_cols |= branch_inherited_all_cols
                    if touches_request:
                        out_all_cols |= _phys_req_star_cols
                    if (
                        len(it_idxs) == 1
                        and len(from_source_schemas) == 1
                        and from_source_schemas[0].positional is not None
                    ):
                        pos_flags.extend(from_source_schemas[0].positional)
                    else:
                        has_unknown_star_item = True
                elif is_qual_star_item:
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
                    if len(it_idxs) == 3 and q_sch.positional is not None:
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
        append_rw = rewritten_tokens.append
        sig_pos_by_idx = proc_sig_pos
        sig_len = len(sig_idxs)
        has_col_spans = bool(col_list_table_spans)
        has_auto_alias = bool(auto_alias_after_idx)
        rewritable_idents = _REQUEST_CF_FIELDS | frozenset(table_map) | frozenset({"timestamp", "ip"})
        skip_until_idx = -1
        for idx, tok_pair in enumerate(processed):
            if idx <= skip_until_idx:
                continue
            if has_col_spans and idx in col_list_table_spans:
                end_span_idx, ph_span = col_list_table_spans[idx]
                append_rw(("SUBQ", ph_span))
                skip_until_idx = end_span_idx
                continue
            k, t = tok_pair
            if k != "IDENT":
                if idx in table_token_idxs:
                    append_rw(("IDENT", table_token_idxs[idx]))
                else:
                    append_rw(tok_pair)
                continue
            if idx in table_token_idxs:
                append_rw(("IDENT", table_token_idxs[idx]))
                continue
            if len(t) < 2 or t.lower() not in rewritable_idents or idx in table_alias_idxs or idx in proj_alias_idxs:
                append_rw(tok_pair)
                continue
            low_ident = t.lower()
            s_pos = sig_pos_by_idx[idx]
            prev_tok = processed[sig_idxs[s_pos - 1]] if s_pos > 0 else None
            next_tok = processed[sig_idxs[s_pos + 1]] if s_pos + 1 < sig_len else None
            if (
                prev_tok is not None
                and prev_tok[0] == "IDENT"
                and len(prev_tok[1]) == 2
                and prev_tok[1].upper() == "AS"
                and not _is_non_kw_lex_pos(processed, sig_idxs[s_pos - 1])
            ):
                append_rw(tok_pair)
                continue
            if next_tok == ("OP", "("):
                append_rw(tok_pair)
                continue
            if next_tok == ("OP", "."):
                low_q = t.lower()
                if low_q in table_map and low_q not in ctes:
                    append_rw(("IDENT", table_map[low_q][0]))
                else:
                    append_rw(tok_pair)
                continue
            if prev_tok == ("OP", "."):
                qual_tok = processed[sig_idxs[s_pos - 2]] if s_pos >= 2 else None
                q_domains = (
                    alias_domains.get(_unquote_sql_ident(qual_tok[1]).lower(), domains)
                    if (qual_tok and qual_tok[0] in ("IDENT", "DQUOTE"))
                    else domains
                )
                low_c = t.lower()
                alias_suffix = (
                    f" AS {auto_alias_after_idx[idx]}" if (has_auto_alias and idx in auto_alias_after_idx) else ""
                )
                if low_c == "timestamp" and bool(q_domains & raw_fact_domains):
                    append_rw(("IDENT", f"event_timestamp{alias_suffix}"))
                elif low_c == "ip" and "request" in q_domains:
                    append_rw(("IDENT", f"client_ip{alias_suffix}"))
                elif low_c in _REQUEST_CF_FIELDS and "request" in q_domains:
                    append_rw(("IDENT", f"custom_fields['{low_c}']{alias_suffix}"))
                else:
                    append_rw(tok_pair)
                continue
            low_ident = t.lower()
            alias_suffix = (
                f" AS {auto_alias_after_idx[idx]}" if (has_auto_alias and idx in auto_alias_after_idx) else ""
            )
            if low_ident == "timestamp" and touches_any_fact:
                append_rw(("IDENT", f"event_timestamp{alias_suffix}"))
            elif low_ident == "ip" and touches_request:
                append_rw(("IDENT", f"client_ip{alias_suffix}"))
            elif low_ident in _REQUEST_CF_FIELDS and touches_request:
                append_rw(("IDENT", f"custom_fields['{low_ident}']{alias_suffix}"))
            else:
                append_rw(tok_pair)

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
        if subquery_placeholders:
            branch_sql = re.sub(
                rf"__fla_subq_{sub_nonce}_[0-9]+__",
                lambda m: subquery_placeholders[m.group(0)],
                branch_sql,
            )

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

    if _FAST_CON_LEX_SPECIAL_RE.search(sql) is None:
        tokens = [(m.lastgroup or "OTHER", m.group(0)) for m in _FAST_CON_LEX_RE.finditer(sql)]
    else:
        tokens = [(m.lastgroup or "OTHER", m.group(0)) for m in _CONSOLE_SQL_LEX_RE.finditer(sql)]
    _get_lex_meta(tokens, enforce_depth=True)
    rewritten, _, canonicals, _ = _rewrite_tokens(tokens, {}, is_subquery=False)
    return rewritten, canonicals


def _rewrite_console_sql(sql: str, *, svc_table: str, names: Mapping[str, str]) -> str:
    rewritten, _ = _rewrite_console_sql_with_canonicals(sql, svc_table=svc_table, names=names)
    return rewritten


_CONSOLE_EXTRA_SESSION_PII_KEYS = (
    PII_SESSION_KEYS
    | CMCD_SESSION_ALIAS_KEYS
    | frozenset(
        {
            "authorization",
            "proxy_authorization",
            "cookie",
            "set_cookie",
            "x_api_key",
            "api_key",
            "access_token",
            "x_access_token",
            "id_token",
            "refresh_token",
            "auth_token",
            "x_auth_token",
            "csrf_token",
            "xsrf_token",
            "x_csrf_token",
            "x_xsrf_token",
            "session",
            "session_token",
            "x_session_id",
            "x_session_token",
            "cmcd.sid",
            "cmcd.cid",
            "cmcd.session",
            "x_cookie",
            "x_authorization",
            "bearer_token",
            "session_cookie",
        }
    )
)
_CONSOLE_EXTRA_SESSION_NONWORD = frozenset(
    re.sub(r"[^a-z0-9]+", "", k.lower()) for k in _CONSOLE_EXTRA_SESSION_PII_KEYS
)
_CONSOLE_EXTRA_IP_PII_KEYS = PII_IP_KEYS | frozenset(
    {
        "origin_ip",
        "cf_connecting_ip",
        "cf_connecting_ipv6",
        "connecting_ip",
        "connecting_ipv6",
        "x_real_ip",
        "x_client_ip",
        "x_cluster_client_ip",
        "x_original_forwarded_for",
        "x_forwarded_client_ip",
        "forwarded",
        "forwarded_for",
        "src_ip",
        "remote_ip",
        "peer_ip",
        "x_ip",
    }
)
_CONSOLE_EXTRA_IP_NONWORD = frozenset(re.sub(r"[^a-z0-9]+", "", k.lower()) for k in _CONSOLE_EXTRA_IP_PII_KEYS)
_CONSOLE_PII_WS_PY_CHARS = "\x00-\x1f \x7f\x85\u00a0\u1680\u180e\u2000-\u200d\u2028\u2029\u202f\u205f\u2060\u3000\ufeff"
_CONSOLE_PII_WS_STRIP_CHARS = (
    "".join(chr(c) for c in range(0x20))
    + " \x7f\x85\u00a0\u1680\u180e"
    + "".join(chr(c) for c in range(0x2000, 0x200E))
    + "\u2028\u2029\u202f\u205f\u2060\u3000\ufeff"
)
_CONSOLE_PII_CAMEL_IPV_RE = re.compile(r"IP[vV]([46])")
_CONSOLE_PII_CAMEL_RE_1 = re.compile(r"([a-z0-9])([A-Z])")
_CONSOLE_PII_CAMEL_RE_2 = re.compile(r"([A-Z]+)([A-Z][a-z])")
_CONSOLE_PII_SPLIT_OUTER_RE = re.compile(f"[\\[\\]/:;|,&=?{_CONSOLE_PII_WS_PY_CHARS}]+")
_CONSOLE_PII_SPLIT_ALL_RE = re.compile(f"[.\\[\\]/:;|,&=?{_CONSOLE_PII_WS_PY_CHARS}]+")
_CONSOLE_PII_BRACKET_STRIP_RE = re.compile(r"\[[^\[\]]*\]")
_CONSOLE_PII_IPV_RE = re.compile(r"([a-z_])ipv[46](?:_|$)")
_CONSOLE_ARRAY_ELEM_PII_RE = re.compile(
    rf"arrayelement\(\s*(?:(?:{SAFE_IDENT_RE.pattern}|\"(?:[^\"]|\"\")*\"|`(?:[^`]|``)*`)\s*\.\s*){{0,3}}(?:custom_fields|cmcd)\s*,\s*'([^']+)'\s*\)",
    re.IGNORECASE,
)
_CONSOLE_AFFIX_PREFIX_RE = re.compile(
    r"^(?:(?:x_?)?filter_|_?cf_|(?:req|resp|request|response|bereq|beresp|obj)_http_|http_|(?:req|resp|request|response)_|headers?_|(?:raw|orig|original)_)+"
)
_CONSOLE_SUFFIX_RE = re.compile(r"(?:_v[0-9]+|_hash|_addr|_?[0-9]+)+$")


def _normalize_console_pii_unicode(value: str) -> str:
    return value.replace("\u0130", "I").replace("\u0307", "").replace("\u212a", "K")


def _strip_console_pii_brackets(value: str) -> str:
    cur = value
    while True:
        nxt = _CONSOLE_PII_BRACKET_STRIP_RE.sub("", cur)
        if nxt == cur:
            return cur
        cur = nxt


def _classify_console_pii_key(raw_key: Any) -> Literal["session", "ip"] | None:
    if not isinstance(raw_key, str) or not raw_key.strip(_CONSOLE_PII_WS_STRIP_CHARS):
        return None
    seeds = [raw_key]
    for m_ae in _CONSOLE_ARRAY_ELEM_PII_RE.finditer(raw_key):
        if m_ae.group(1).strip(_CONSOLE_PII_WS_STRIP_CHARS):
            seeds.append(m_ae.group(1))
    candidates: list[str] = []
    for seed in seeds:
        low_seed = _normalize_console_pii_unicode(seed).strip(_CONSOLE_PII_WS_STRIP_CHARS).lower()
        if low_seed:
            candidates.append(low_seed)
        decoded = _normalize_console_pii_unicode(unquote(unquote(seed)))
        raw_lower = decoded.strip(_CONSOLE_PII_WS_STRIP_CHARS).lower()
        camel_src = _CONSOLE_PII_CAMEL_IPV_RE.sub(r"Ipv\1", decoded)
        camel = (
            _CONSOLE_PII_CAMEL_RE_2.sub(r"\1_\2", _CONSOLE_PII_CAMEL_RE_1.sub(r"\1_\2", camel_src))
            .strip(_CONSOLE_PII_WS_STRIP_CHARS)
            .lower()
        )
        raw_unbrack = _strip_console_pii_brackets(raw_lower).strip(_CONSOLE_PII_WS_STRIP_CHARS)
        camel_unbrack = _strip_console_pii_brackets(camel).strip(_CONSOLE_PII_WS_STRIP_CHARS)
        for base in (raw_lower, camel, raw_unbrack, camel_unbrack):
            if base:
                candidates.append(base)
                candidates.extend(_CONSOLE_PII_SPLIT_OUTER_RE.split(base))
                candidates.extend(_CONSOLE_PII_SPLIT_ALL_RE.split(base))
    matched_ip = False
    for seg in candidates:
        if not seg:
            continue
        if seg in _CONSOLE_EXTRA_SESSION_PII_KEYS:
            return "session"
        if seg in _CONSOLE_EXTRA_IP_PII_KEYS:
            matched_ip = True
        canon = re.sub(r"[^a-z0-9]+", "_", seg).strip("_")
        nw_under = re.sub(r"[^a-z0-9_]+", "", seg)
        if not canon and not nw_under:
            continue
        ipv_norm = _CONSOLE_PII_IPV_RE.sub(r"\1ip_", canon).strip("_") if canon else ""
        affix_stripped = _CONSOLE_AFFIX_PREFIX_RE.sub("", ipv_norm).strip("_") if ipv_norm else ""
        no_suffix = _CONSOLE_SUFFIX_RE.sub("", ipv_norm).strip("_") if ipv_norm else ""
        stripped = _CONSOLE_SUFFIX_RE.sub("", affix_stripped).strip("_") if affix_stripped else ""
        nw_ipv = _CONSOLE_PII_IPV_RE.sub(r"\1ip_", nw_under) if nw_under else ""
        nw_affix = _CONSOLE_AFFIX_PREFIX_RE.sub("", nw_ipv).strip("_") if nw_ipv else ""
        nw_stripped = _CONSOLE_SUFFIX_RE.sub("", nw_affix).strip("_") if nw_affix else ""
        for cand in (canon, ipv_norm, affix_stripped, no_suffix, stripped, nw_affix, nw_stripped):
            if not cand:
                continue
            nw = cand.replace("_", "")
            if cand in _CONSOLE_EXTRA_SESSION_PII_KEYS or nw in _CONSOLE_EXTRA_SESSION_NONWORD:
                return "session"
            if cand in _CONSOLE_EXTRA_IP_PII_KEYS or nw in _CONSOLE_EXTRA_IP_NONWORD:
                matched_ip = True
    if matched_ip:
        return "ip"
    if is_saved_view_pii_key(raw_key):
        return "session"
    return None


def _mask_nested_extra_pii(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def _mask_scalar(val: Any, kind: Literal["session", "ip"] | None) -> Any:
        if kind is None:
            return val
        if isinstance(val, str):
            if kind == "session":
                return "[redacted]" if val else val
            return val if val == "[redacted]" else mask_ip_or_chain(val)
        if isinstance(val, (int, float)) and not isinstance(val, bool):
            return "[redacted]"
        return val

    def _walk_nested(
        obj: Any,
        *,
        parent_kind: Literal["session", "ip"] | None = None,
    ) -> Any:
        if isinstance(obj, dict):
            out_d: dict[str, Any] = {}
            for k, v in obj.items():
                key_kind = _classify_console_pii_key(k)
                eff_kind = key_kind if key_kind is not None else parent_kind
                if isinstance(v, (dict, list, tuple)):
                    out_d[k] = _walk_nested(v, parent_kind=eff_kind)
                else:
                    out_d[k] = _mask_scalar(v, eff_kind)
            return out_d
        if isinstance(obj, list):
            return [_walk_nested(x, parent_kind=parent_kind) for x in obj]
        if isinstance(obj, tuple):
            return tuple(_walk_nested(x, parent_kind=parent_kind) for x in obj)
        return _mask_scalar(obj, parent_kind)

    return [_walk_nested(r, parent_kind=None) for r in rows]


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
            elif (
                mask_ips
                and not isinstance(v, (dict, list, tuple))
                and (bool(v) or (isinstance(v, (int, float)) and not isinstance(v, bool)))
                and _classify_console_pii_key(str(k)) is not None
            ):
                row_copy[k] = "[redacted]"
        data.append(row_copy)
    if mask_ips:
        data = apply_pii_policy(data, {"mask_ips": True})
        data = _mask_nested_extra_pii(data)
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
