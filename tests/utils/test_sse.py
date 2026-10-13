from collections.abc import AsyncIterator

from sse_starlette.sse import EventSourceResponse

from backend.utils.router_utils import SSE_PASSTHROUGH_HEADERS
from backend.utils.sse import sse_response


async def _stream() -> AsyncIterator[str]:
    yield "data: hello\n\n"


def _headers(resp: EventSourceResponse) -> dict[str, str]:
    return {k.decode().lower(): v.decode() for k, v in resp.raw_headers}


def test_sse_response_returns_event_source_response():
    resp = sse_response(_stream())
    assert isinstance(resp, EventSourceResponse)


def test_sse_response_default_ping_is_5():
    resp = sse_response(_stream())
    assert resp._ping_interval == 5


def test_sse_response_ping_passthrough():
    resp = sse_response(_stream(), ping=11)
    assert resp._ping_interval == 11


def test_sse_response_applies_passthrough_headers():
    resp = sse_response(_stream())
    headers = _headers(resp)
    for key, value in SSE_PASSTHROUGH_HEADERS.items():
        assert headers[key.lower()] == value
