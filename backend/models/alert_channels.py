"""Pydantic model for alert notification channels."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel


class AlertChannel(BaseModel):
    type: Literal["slack", "pagerduty", "webhook"]
    url: str
    config: dict | None = None
