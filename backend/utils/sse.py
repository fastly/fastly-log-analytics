"""Shared ``EventSourceResponse`` factory for SSE endpoints.

Every SSE route in the app returns the identical
``EventSourceResponse(stream(), ping=5, headers=SSE_PASSTHROUGH_HEADERS)``.
``sse_response()`` centralizes that shape so the ping interval and the
proxy-passthrough headers (Fastly / Caddy / nginx no-buffer hints) live in one
place; the per-endpoint ``stream()`` closure — including its own
cancellation / cleanup ``finally`` block — is passed in unchanged.

This module is in ``backend.utils`` on purpose: it must not import routers
(import-contracts forbid routers↛routers edges), so the helper takes the async
generator and returns the response without reaching back into route code.
"""

from __future__ import annotations

from collections.abc import AsyncIterable, Iterator
from typing import Any

from sse_starlette.sse import EventSourceResponse

from backend.utils.router_utils import SSE_PASSTHROUGH_HEADERS


def sse_response(stream: AsyncIterable[Any] | Iterator[Any], *, ping: int = 5) -> EventSourceResponse:
    """Wrap an SSE ``stream`` in an ``EventSourceResponse``.

    Applies the shared ``ping`` interval (default 5s, matching every existing
    call site) and ``SSE_PASSTHROUGH_HEADERS`` so no caller has to repeat them.
    Event framing / payloads are produced entirely by ``stream`` and are left
    untouched.
    """
    return EventSourceResponse(stream, ping=ping, headers=SSE_PASSTHROUGH_HEADERS)
