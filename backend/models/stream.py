"""Pydantic request and response models for Live Log Streaming."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class StreamLogEvent(BaseModel):
    """Event payload emitted by the live log stream."""

    event_id: str
    timestamp: str
    client_ip: str
    method: str = "GET"
    url: str = "/"
    status: int = 200
    pop: str = ""
    time_elapsed: float = 0.0
    attributes: dict[str, Any] = Field(default_factory=dict)


class StreamFilterParams(BaseModel):
    """Optional query filters supported by the live log stream."""

    filter: str | None = None
    status: str | None = None
    pop: str | None = None
    client_ip: str | None = None
