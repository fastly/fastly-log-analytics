from __future__ import annotations

from datetime import UTC, datetime

import pytest

from scripts.dev.scale_harness import (
    freshness_lag_seconds,
    parse_stages,
    render_report,
)


def test_parse_stages_and_high_rate_gate() -> None:
    stages = parse_stages("100:10,500:2")
    assert [(stage.target_rps, stage.duration_s) for stage in stages] == [
        (100, 10),
        (500, 2),
    ]


def test_parse_stages_rejects_high_rate_without_explicit_flag() -> None:
    with pytest.raises(ValueError, match="allow-high-rate"):
        parse_stages("2500:10")


def test_parse_stages_accepts_high_rate_with_explicit_flag() -> None:
    stages = parse_stages("2500:10", allow_high_rate=True)
    assert stages[0].target_rps == 2500


def test_freshness_lag_is_separate_from_missing_data() -> None:
    now = datetime(2026, 9, 6, 17, 42, 35, tzinfo=UTC)
    assert freshness_lag_seconds("2026-09-06T17:32:08Z", now) == 627.0
    assert freshness_lag_seconds(None, now) is None


def test_render_report_does_not_turn_missing_lag_into_zero() -> None:
    report = {
        "started_at": "2026-09-06T17:00:00+00:00",
        "stages": [
            {
                "index": 1,
                "result": {
                    "target_rps": 100,
                    "achieved_rps": 98,
                    "completed": 10,
                    "successful": 10,
                    "latency_ms": {"p95": 20},
                },
                "checkpoint": {},
            }
        ],
    }
    assert "| 1 | 100 | 98.00 | 10 | 10 | 20 | - |" in render_report(report)
