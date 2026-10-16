"""Server-sent-event parsing for route tests."""

from __future__ import annotations

import json


def parse_sse_events(text: str) -> list[dict]:
    """Decode every ``data:`` event in an SSE body into a dict, skipping non-JSON payloads."""
    out: list[dict] = []
    for chunk in text.replace("\r\n", "\n").split("\n\n"):
        chunk = chunk.strip()
        if chunk.startswith("data:"):
            try:
                out.append(json.loads(chunk[len("data:") :].strip()))
            except json.JSONDecodeError:
                pass
    return out
