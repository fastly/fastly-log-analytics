from __future__ import annotations

import json

from backend.high_scale.archive_models import ArchiveSourceObject
from backend.high_scale.decoder import decode_source_object


def test_request_field_normalization_parity_with_field_registry() -> None:
    """Verifies that decoder normalization adheres to types defined in field_registry."""
    source = ArchiveSourceObject(
        service_id="test_service",
        domain="request",
        object_key="raw/request/test.log.gz",
        checksum="sha256:123",
        size_bytes=100,
    )
    payload = json.dumps(
        {
            "timestamp": "2026-10-09T12:00:00Z",
            "client_ip": "192.0.2.1",
            "status": "200",
            "resp_body_size": "1024",
            "url": "/test",
            "fastly_pop": "SFO",
            "custom_header": "custom_val",
            "cmcd_br": "2000",
        }
    ).encode("utf-8")

    result = decode_source_object(source, payload, transform_version="request.v1")
    assert result.accepted_rows == 1
    event = result.events[0]

    # Status must be normalized to integer consistent with registry
    assert isinstance(event["status"], int) or event["status"] == 200
    # resp_body_size must be normalized to integer
    assert isinstance(event["resp_body_size"], int) or event["resp_body_size"] == 1024
    # Pop and IP string normalization
    assert event["fastly_pop"] == "SFO"
    assert event["client_ip"] == "192.0.2.1"


def test_unknown_fields_and_cmcd_routed_to_maps() -> None:
    source = ArchiveSourceObject(
        service_id="test_service",
        domain="request",
        object_key="raw/request/test.log.gz",
        checksum="sha256:123",
        size_bytes=100,
    )
    payload = json.dumps(
        {
            "timestamp": "2026-10-09T12:00:00Z",
            "url": "/stream.mpd",
            "user_tag": "vip",
            "cmcd_sid": "session-1",
            "cmcd_br": "5000",
        }
    ).encode("utf-8")

    result = decode_source_object(source, payload, transform_version="request.v1")
    assert result.accepted_rows == 1
    event = result.events[0]

    # Verify custom_fields map and cmcd map separation
    assert "custom_fields" in event
    assert "cmcd" in event
    assert event["custom_fields"].get("user_tag") == "vip"
    assert event["cmcd"].get("sid") == "session-1" or event["cmcd"].get("cmcd_sid") == "session-1"
