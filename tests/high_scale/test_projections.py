import pytest

from backend.high_scale.projections import project_cmcd


def test_cmcd_projection_preserves_request_identity_and_full_session_id() -> None:
    request = {
        "event_id": "request-event-1",
        "service_id": "svc",
        "timestamp": "2026-09-01T00:00:00Z",
        "cmcd_version": "v2",
        "sid": "session-" + "x" * 64,
        "cmcd": {"br": 1200, "d": 4000},
    }

    projection = project_cmcd(request)

    assert projection is not None
    assert projection["request_event_id"] == "request-event-1"
    assert projection["projection_key"]
    assert projection["sid"] == request["sid"]
    assert projection["cmcd_version"] == "v2"
    assert projection["fields"] == {"br": 1200, "d": 4000}


def test_cmcd_projection_is_absent_when_request_has_no_cmcd() -> None:
    assert project_cmcd({"event_id": "request-event-1", "service_id": "svc"}) is None


def test_cmcd_projection_requires_request_event_identity() -> None:
    with pytest.raises(ValueError, match="request_event_id"):
        project_cmcd({"service_id": "svc", "cmcd": {"br": 1200}})
