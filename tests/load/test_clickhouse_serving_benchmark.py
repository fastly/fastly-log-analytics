"""Safety and measurement contracts; no Docker or network required."""

import asyncio
import copy
import json
from datetime import UTC, datetime
from unittest.mock import patch

import httpx
import pytest
from pydantic import ValidationError

from tests.load.clickhouse_serving_benchmark import (
    BenchmarkReport,
    Freshness,
    RecoveryEvidence,
    Sample,
    Stage,
    attempt,
    parser,
    quantile,
    request_for,
    runtime,
    summarize_payload,
    validate_args,
)


def args(*extra):
    return parser().parse_args(
        [
            "--engine",
            "ducklake",
            "--output",
            "local-docs/test-evidence.json",
            "--apply",
            "--allow-prototype",
            "--base-url",
            "http://127.0.0.1",
            "--backend-container",
            "local-backend",
            *extra,
        ]
    )


@pytest.mark.parametrize(
    "url",
    [
        "https://127.0.0.1",
        "http://localhost",
        "http://example.com",
        "http://127.0.0.1.example.com",
        "http://user:secret@127.0.0.1",
        "http://127.0.0.1/path",
        "http://127.0.0.1?q=x",
    ],
)
def test_nonlocal_and_ambiguous_origins_refused(url):
    with pytest.raises(ValueError):
        validate_args(args("--base-url", url))


@pytest.mark.parametrize(
    "flag,value",
    [
        ("--attempts", "29"),
        ("--attempts", "257"),
        ("--timeout", "0"),
        ("--timeout", "61"),
        ("--timeout", "nan"),
        ("--stage-seconds", "601"),
        ("--memory-abort-mb", "2049"),
        ("--concurrency", "128"),
        ("--output", "report.json"),
    ],
)
def test_bounds_refused(flag, value):
    with pytest.raises(ValueError):
        validate_args(args(flag, value))


@pytest.mark.parametrize("flag", ["apply", "allow_prototype", "base_url", "backend_container"])
def test_explicit_optin_required(flag):
    a = args()
    setattr(a, flag, None)
    with pytest.raises(ValueError):
        validate_args(a)


def test_allowed_budget_and_quantiles():
    with patch("subprocess.run") as run:
        run.return_value.returncode = 0
        validate_args(args())
    assert quantile([1.0, 2.0, 100.0], 0.95) == pytest.approx(90.2)


def test_live_hybrid_benchmark_refused_before_docker_or_network():
    with patch("subprocess.run") as run:
        with pytest.raises(ValueError, match="archived"):
            validate_args(args("--engine", "clickhouse_hybrid"))
        run.assert_not_called()


def test_removed_dashboard_route_cannot_be_reported_as_hybrid():
    payload = top_n_payload()
    payload["_section_timings"] = [{"section": "agg:query", "time_ms": 1.0}]
    summary = summarize_payload(payload, "top_n", "clickhouse_hybrid")
    assert summary["error"] == "engine"
    assert summary["clickhouse_calls"] == 0


def test_current_ducklake_dashboard_needs_no_retired_dispatch_marker():
    payload = top_n_payload()
    payload["_section_timings"] = [{"section": "agg:query", "time_ms": 1.0}]
    summary = summarize_payload(payload, "top_n", "ducklake")
    assert summary["error"] is None
    assert summary["dispatch"] == "ducklake"
    assert summary["duckdb_queries"] == 1


@pytest.mark.parametrize("enabled", ["true", "false"])
def test_ducklake_benchmark_does_not_require_disabling_diagnostics(enabled):
    container = {
        "Config": {
            "Env": [
                "DEPLOYMENT_MODE=high_scale",
                f"CLICKHOUSE_ENABLED={enabled}",
                "METADATA_DSN=postgresql://fixture",
                "DUCKLAKE_CATALOG=postgresql://fixture",
                "CLICKHOUSE_HOST=clickhouse",
            ],
            "Labels": {
                "com.docker.compose.project.config_files": (
                    "docker-compose.multipod.yml,docker-compose.observability.yml,"
                    "docker-compose.clickhouse-prototype.yml"
                )
            },
        },
        "Image": "sha256:" + "a" * 64,
        "HostConfig": {"Memory": 0},
    }
    with patch("subprocess.check_output", return_value=json.dumps([container])):
        assert runtime("local-backend") == (container["Image"], 0)


def test_mixed_corpus_preserves_narrow_adapter_support():
    assert request_for("bundle")[1]["sections"] == ["core", "topten"]
    assert request_for("top_n")[1]["sections"] == ["topten"]
    assert request_for("time_series")[1]["include_conn_requests"] is False
    assert request_for("empty")[1]["start_time"] == "2026-09-06T00:00:00Z"


@pytest.mark.parametrize("status", [429, 500, 503])
def test_failures_retained_and_response_secrets_omitted(status):
    async def run():
        async with httpx.AsyncClient(
            base_url="http://127.0.0.1",
            transport=httpx.MockTransport(
                lambda _: httpx.Response(status, json={"secret": "NEVER_COPY_SECRET", "sql": "SELECT PRIVATE"})
            ),
        ) as client:
            return await attempt(client, "PRIVATE_SERVICE", "bundle", "ducklake", 1)

    sample = asyncio.run(run())
    assert sample.error == "http" and sample.status_code == status and sample.duration_ms > 0
    assert sample.rows_read is None and sample.bytes_read is None and sample.readiness_ms is None
    assert "PRIVATE" not in sample.model_dump_json() and "SECRET" not in sample.model_dump_json()


def test_deadline_retains_slow_attempt():
    async def handler(_):
        await asyncio.sleep(1)
        return httpx.Response(200)

    async def run():
        async with httpx.AsyncClient(base_url="http://127.0.0.1", transport=httpx.MockTransport(handler)) as client:
            return await attempt(client, "fixture", "bundle", "ducklake", 0.01)

    sample = asyncio.run(run())
    assert sample.error == "timeout" and sample.duration_ms >= 10


def test_schema_rejects_arbitrary_payload_and_false_complete():
    with pytest.raises(ValidationError):
        Sample.model_validate({"sql": "PRIVATE"})
    with pytest.raises(ValidationError):
        Stage(
            concurrency=1,
            requested_attempts=30,
            peak_in_flight=1,
            status="complete",
            elapsed_seconds=1.0,
            backend_peak_memory_bytes=1,
            warmups=[],
            samples=[],
            report=None,
        )
    assert "sql" not in json.dumps(Sample.model_json_schema()["properties"])


def top_n_payload(dispatch="disabled"):
    return {
        "_is_cached": False,
        "_section_timings": [{"section": "engine:" + dispatch, "time_ms": 0.0}],
        "_debug_queries": [{"sql": "PRIVATE_SQL", "time_ms": 1.0}],
        "total_rows": 184374,
        "interval": "1 minute",
        "metric": "requests",
        "data": {"country": {"total": 184374, "top": [{"value": "US", "count": 184374, "label": None}]}},
        "time_series": [],
        "map_data": [],
    }


def test_top_n_has_no_chart_and_uses_existing_minute_metadata():
    summary = summarize_payload(top_n_payload(), "top_n", "ducklake")
    assert summary["error"] is None
    assert summary["semantic_digest"] is not None
    assert "PRIVATE" not in json.dumps(summary)


def test_disabled_engine_must_not_be_mislabeled_fallback():
    summary = summarize_payload(top_n_payload("unsupported_sections"), "top_n", "ducklake")
    assert summary["error"] == "engine"
    assert summarize_payload(top_n_payload("unsupported_sections"), "top_n", "clickhouse_hybrid")["error"] is None


def test_telemetry_counts_and_errors_are_retained_without_details():
    payload = top_n_payload("unsupported_sections")
    payload["_debug_calls"] = [
        {
            "service": "ClickHouse",
            "url": "PRIVATE_HOST",
            "details": json.dumps(
                {
                    "query_id": "PRIVATE_ID",
                    "outcome": "error",
                    "rows_read": 2,
                    "bytes_read": 100,
                }
            ),
        }
    ]
    summary = summarize_payload(payload, "top_n", "clickhouse_hybrid")
    assert summary["clickhouse_errors"] == 1
    assert summary["rows_read"] == 2 and summary["bytes_read"] == 100
    assert "PRIVATE" not in json.dumps(summary)


@pytest.mark.parametrize("memory", [1024, 2 * 1024**3])
def test_scheduler_respects_budget_and_records_errors(monkeypatch, memory):
    from tests.load import clickhouse_serving_benchmark as module

    calls = 0

    async def fake_attempt(*_):
        nonlocal calls
        calls += 1
        n = calls
        await asyncio.sleep(0)
        return Sample(
            kind="bundle",
            duration_ms=float(n),
            status_code=200 if n <= 3 else 503,
            error=None if n <= 3 else "http",
            dispatch="disabled",
            response_cached=False,
            duckdb_queries=0,
            clickhouse_calls=0,
            clickhouse_errors=0,
            rows_read=0,
            bytes_read=0,
            readiness_ms=0.0,
            semantic_digest=None,
        )

    async def fake_memory(*_):
        return memory

    monkeypatch.setattr(module, "attempt", fake_attempt)
    monkeypatch.setattr(module, "memory_bytes", fake_memory)
    now = datetime(2026, 9, 7, tzinfo=UTC)
    fresh = Freshness(
        observed_at=now,
        event_watermark=now,
        window_start=datetime(2026, 9, 1, tzinfo=UTC),
        window_end=datetime(2026, 9, 2, tzinfo=UTC),
        last_commit_at=now,
        last_publication_at=now,
    )
    measured = asyncio.run(module.stage(None, "fixture", args(), 8, fresh, 0.0))
    if memory > 1024:
        assert measured.status == "memory_abort"
        assert measured.samples == [] and measured.report is None and calls == 0
    else:
        assert measured.status == "complete" and measured.peak_in_flight == 8
        assert len(measured.samples) == 64 and calls == 67
        assert measured.report.error_count == 64 and measured.report.achieved_throughput_rps == 0
        assert measured.max_ms == 67
        assert measured.report.p99_ms == quantile([float(n) for n in range(4, 68)], 0.99)


def test_recovery_contract_never_accepts_credentials_or_nonfinite_durations():
    with pytest.raises(ValidationError):
        RecoveryEvidence.model_validate({"password": "NEVER_COPY"})
    assert "password" not in RecoveryEvidence.model_json_schema()["properties"]


# ── Merged from tests/load/test_clickhouse_serving_contract.py ──


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
