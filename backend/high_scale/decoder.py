"""Lossless source-object decoding for the high-scale ingestion boundary."""

from __future__ import annotations

import base64
import gzip
import json
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qs, urlparse

from backend.core.field_registry import DuckType, try_get
from backend.high_scale.archive_models import ArchiveSourceObject
from backend.high_scale.schema import build_event_id, build_request_event_id, build_rum_event_id


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
    source: ArchiveSourceObject,
    payload: bytes,
    *,
    transform_version: str,
    domain: str | None = None,
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
            event = dict(value)
            event.update(
                {
                    "service_id": source.service_id,
                    "domain": selected_domain,
                    "source_object_key": source.object_key,
                    "source_object_version": source_version,
                    "line_ordinal": line_ordinal,
                    "transform_version": transform_version,
                    "raw_json": raw_line.decode("utf-8"),
                }
            )
            _normalize_serving_fields(event, selected_domain)
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

        # The edge-extracted flat rum_metric_* fields can legitimately come
        # through empty (edge-snippet ordering/case drift) while the raw
        # beacon querystring survives in rum_raw_query/url — mirrors the
        # standard-tier fallback in backend/core/ingest.py.
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
        event["error_file"] = event.get("rum_error_file") or event.get("error_file") or ""
        event["pathname"] = event.get("rum_pathname") or event.get("pathname") or ""


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


def _event_id(event: dict[str, Any], domain: str) -> str:
    if domain == "request":
        return build_request_event_id(event)
    if domain == "rum_vitals":
        return build_rum_event_id(event, rum_kind="vitals")
    if domain == "rum_errors":
        return build_rum_event_id(event, rum_kind="errors")
    return build_event_id(event, domain=domain)
