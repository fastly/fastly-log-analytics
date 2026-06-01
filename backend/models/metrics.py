"""Shared metric definitions."""

from __future__ import annotations

from enum import StrEnum


class MetricType(StrEnum):
    """Available metrics for timeseries and analysis endpoints."""

    REQUESTS = "requests"
    ERRORS_5XX = "5xx"
    ERRORS_4XX = "4xx"
    HIT_RATE = "hit_rate"
    LATENCY_P95 = "p95_latency"
    THROUGHPUT = "throughput"
    REQ_SIZE = "req_size"
    TTFB = "ttfb"
    SPECIFIC_STATUS = "specific_status"


class ChartInterval(StrEnum):
    """Allowed bucket sizes for timeseries charts."""

    SECOND_1 = "1 second"
    MINUTE_1 = "1 minute"
    HOUR_1 = "1 hour"
    DAY_1 = "1 day"
