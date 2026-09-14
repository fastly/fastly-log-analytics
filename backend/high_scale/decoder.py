"""Lossless source-object decoding for the high-scale ingestion boundary."""

from __future__ import annotations

import base64
import gzip
import json
from dataclasses import dataclass
from typing import Any

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


def _event_id(event: dict[str, Any], domain: str) -> str:
    if domain == "request":
        return build_request_event_id(event)
    if domain == "rum_vitals":
        return build_rum_event_id(event, rum_kind="vitals")
    if domain == "rum_errors":
        return build_rum_event_id(event, rum_kind="errors")
    return build_event_id(event, domain=domain)
