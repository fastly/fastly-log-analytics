from __future__ import annotations

from typing import Any

from backend.models.common import BaseResponse


class PerformanceAggregatesResponse(BaseResponse):
    top_urls: list[dict[str, Any]] = []
    top_asns: list[dict[str, Any]] = []
    ttl_dist: list[dict[str, Any]] = []
    scatter: list[dict[str, Any]] = []
    waterfall: dict[str, dict[str, float]] = {}


class PerformanceOriginTsResponse(BaseResponse):
    timeseries: list[dict[str, Any]] = []
