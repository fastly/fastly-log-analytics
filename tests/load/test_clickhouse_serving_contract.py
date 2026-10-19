"""The shared report contract must reject incomplete or misleading measurements."""

import copy
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from tests.load.clickhouse_serving_contract import BenchmarkReport


@pytest.fixture
def report():
    return {
        "schema_version": 1,
        "engine": "ducklake",
        "dataset_size_rows": 190334,
        "eligible_window_rows": 184374,
        "concurrency": 1,
        "warmup_requests": 3,
        "measured_requests": 30,
        "elapsed_seconds": 3.0,
        "achieved_throughput_rps": 10.0,
        "p50_ms": 90.0,
        "p95_ms": 110.0,
        "p99_ms": 120.0,
        "ingest_lag_seconds": 128.0,
        "error_count": 0,
        "freshness": {
            "observed_at": datetime(2026, 9, 7, 23, tzinfo=UTC),
            "event_watermark": datetime(2026, 9, 7, 19, tzinfo=UTC),
            "window_start": datetime(2026, 9, 1, tzinfo=UTC),
            "window_end": datetime(2026, 9, 2, tzinfo=UTC),
            "last_commit_at": datetime(2026, 9, 7, 20, tzinfo=UTC),
            "last_publication_at": datetime(2026, 9, 7, 21, tzinfo=UTC),
        },
        "clickhouse_ingest_lag_seconds": None,
        "clickhouse_rebuild_seconds": None,
    }


def test_report_json_roundtrip(report):
    validated = BenchmarkReport.model_validate(report)
    assert BenchmarkReport.model_validate_json(validated.model_dump_json()) == validated
    assert validated.model_dump()["clickhouse_rebuild_seconds"] is None


@pytest.mark.parametrize(
    "field",
    [
        "schema_version",
        "engine",
        "dataset_size_rows",
        "eligible_window_rows",
        "concurrency",
        "warmup_requests",
        "measured_requests",
        "elapsed_seconds",
        "achieved_throughput_rps",
        "p50_ms",
        "p95_ms",
        "p99_ms",
        "ingest_lag_seconds",
        "error_count",
        "freshness",
        "clickhouse_ingest_lag_seconds",
        "clickhouse_rebuild_seconds",
    ],
)
def test_missing_report_field_rejected(report, field):
    del report[field]
    with pytest.raises(ValidationError):
        BenchmarkReport.model_validate(report)


@pytest.mark.parametrize("value", [-1, float("nan"), float("inf"), -float("inf"), "1", True, None])
@pytest.mark.parametrize(
    "field", ["elapsed_seconds", "achieved_throughput_rps", "p50_ms", "p95_ms", "p99_ms", "ingest_lag_seconds"]
)
def test_invalid_measurement_rejected(report, field, value):
    report[field] = value
    with pytest.raises(ValidationError):
        BenchmarkReport.model_validate(report)


@pytest.mark.parametrize("value", [-1, 1.5, True, "30", None])
@pytest.mark.parametrize(
    "field",
    ["dataset_size_rows", "eligible_window_rows", "concurrency", "warmup_requests", "measured_requests", "error_count"],
)
def test_invalid_count_rejected(report, field, value):
    report[field] = value
    with pytest.raises(ValidationError):
        BenchmarkReport.model_validate(report)


@pytest.mark.parametrize(
    "updates",
    [
        {"p50_ms": 111.0},
        {"p99_ms": 109.0},
        {"error_count": 31},
        {"eligible_window_rows": 190335},
        {"dataset_size_rows": 0},
        {"eligible_window_rows": 0},
        {"concurrency": 0},
        {"warmup_requests": 2},
        {"measured_requests": 29},
        {"elapsed_seconds": 0.0},
        {"achieved_throughput_rps": 0.0},
        {"clickhouse_ingest_lag_seconds": 0.0},
        {"clickhouse_rebuild_seconds": 0.0},
        {"schema_version": True},
        {"engine": "unknown"},
        {"unexpected": "not allowed"},
    ],
)
def test_inconsistent_report_rejected(report, updates):
    report.update(updates)
    with pytest.raises(ValidationError):
        BenchmarkReport.model_validate(report)


@pytest.mark.parametrize(
    "field", ["observed_at", "event_watermark", "window_start", "window_end", "last_commit_at", "last_publication_at"]
)
def test_missing_or_naive_freshness_rejected(report, field):
    missing = copy.deepcopy(report)
    del missing["freshness"][field]
    with pytest.raises(ValidationError):
        BenchmarkReport.model_validate(missing)
    report["freshness"][field] = datetime(2026, 9, 1)
    with pytest.raises(ValidationError):
        BenchmarkReport.model_validate(report)


@pytest.mark.parametrize(
    "field,value",
    [
        ("window_end", datetime(2026, 9, 1, tzinfo=UTC)),
        ("event_watermark", datetime(2026, 9, 8, tzinfo=UTC)),
        ("last_commit_at", datetime(2026, 9, 8, tzinfo=UTC)),
        ("last_publication_at", datetime(2026, 9, 8, tzinfo=UTC)),
        ("unexpected", datetime(2026, 9, 1, tzinfo=UTC)),
    ],
)
def test_invalid_freshness_order_rejected(report, field, value):
    report["freshness"][field] = value
    with pytest.raises(ValidationError):
        BenchmarkReport.model_validate(report)


def test_errors_reduce_successful_throughput(report):
    report.update(error_count=3, achieved_throughput_rps=9.0)
    assert BenchmarkReport.model_validate(report).error_count == 3
    report["achieved_throughput_rps"] = 10.0
    with pytest.raises(ValidationError):
        BenchmarkReport.model_validate(report)


def test_equal_quantiles_and_all_failed_requests_are_representable(report):
    report.update(p50_ms=0.0, p95_ms=0.0, p99_ms=0.0, error_count=30, achieved_throughput_rps=0.0)
    assert BenchmarkReport.model_validate(report).error_count == 30


def test_later_hybrid_measurements_are_explicit(report):
    report.update(engine="clickhouse_hybrid", clickhouse_ingest_lag_seconds=1.0, clickhouse_rebuild_seconds=42.0)
    assert BenchmarkReport.model_validate(report).clickhouse_rebuild_seconds == 42.0
    report["clickhouse_rebuild_seconds"] = float("nan")
    with pytest.raises(ValidationError):
        BenchmarkReport.model_validate(report)
