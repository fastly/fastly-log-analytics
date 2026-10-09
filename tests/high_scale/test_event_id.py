import uuid
from typing import Any

from backend.high_scale.schema import (
    build_cmcd_projection_key,
    build_request_event_id,
    build_rum_event_id,
)


def _base_row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "service_id": "svc_test",
        "domain": "request",
        "source_object_key": "raw/request/2026-10-09/file1.gz",
        "source_object_version": "v1_etag",
        "line_ordinal": 0,
        "transform_version": "request.v1",
    }
    row.update(overrides)
    return row


def test_event_id_is_128_bit_valid_uuid() -> None:
    row = _base_row()
    event_id = build_request_event_id(row)

    # Must be valid UUID representation (128-bit hash)
    parsed = uuid.UUID(event_id)
    assert str(parsed) == event_id or event_id == parsed.hex
    # 128-bit means 16 bytes / 32 hex chars (or 36 chars with hyphens)
    assert len(parsed.bytes) == 16


def test_event_id_is_deterministic_and_stable_across_retries() -> None:
    row1 = _base_row(line_ordinal=42)
    row2 = _base_row(line_ordinal=42)
    assert build_request_event_id(row1) == build_request_event_id(row2)


def test_event_id_differs_by_line_ordinal() -> None:
    row1 = _base_row(line_ordinal=0)
    row2 = _base_row(line_ordinal=1)
    assert build_request_event_id(row1) != build_request_event_id(row2)


def test_event_id_is_length_prefix_unambiguous() -> None:
    # ("ab", "c") vs ("a", "bc") must produce distinct event_ids to prevent delimiter collision
    row1 = _base_row(service_id="ab", source_object_key="c")
    row2 = _base_row(service_id="a", source_object_key="bc")
    assert build_request_event_id(row1) != build_request_event_id(row2)


def test_event_id_differs_by_source_version() -> None:
    row1 = _base_row(source_object_version="etag_v1")
    row2 = _base_row(source_object_version="etag_v2")
    assert build_request_event_id(row1) != build_request_event_id(row2)


def test_event_id_differs_by_transform_version() -> None:
    row1 = _base_row(transform_version="request.v1")
    row2 = _base_row(transform_version="request.v2")
    assert build_request_event_id(row1) != build_request_event_id(row2)


def test_rum_event_ids_separate_vitals_and_errors() -> None:
    row = _base_row()
    vitals_id = build_rum_event_id(row, rum_kind="vitals")
    errors_id = build_rum_event_id(row, rum_kind="errors")
    assert vitals_id != errors_id
    assert uuid.UUID(vitals_id)
    assert uuid.UUID(errors_id)


def test_cmcd_projection_key_deterministic_128_bit() -> None:
    req_id = build_request_event_id(_base_row())
    cmcd_key1 = build_cmcd_projection_key({"request_event_id": req_id})
    cmcd_key2 = build_cmcd_projection_key({"request_event_id": req_id})
    assert cmcd_key1 == cmcd_key2
    # Verify valid 128-bit UUID or 32-char hex
    assert len(uuid.UUID(cmcd_key1).bytes) == 16
