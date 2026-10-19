"""Opt-in real ClickHouse SQL gate, with no ports or deployed infrastructure.

RUN_RUM_CLICKHOUSE_INTEGRATION=1 uv run pytest tests/high_scale/test_rum_integration.py
Uses an already-installed Docker image and disposable clickhouse-local processes.
The production instrumented HTTP client runs against a local transport executing
each statement on that engine with the canonical fact schema and fixture data.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from email.parser import BytesParser
from email.policy import default
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
import pytest

from backend.config import ClickHouseConfig
from backend.core.clickhouse_client import ClickHouseClient
from backend.high_scale.archive_models import ServingWatermark
from backend.high_scale.registry import HighScaleService
from backend.high_scale.rum import rum_analytics

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_RUM_CLICKHOUSE_INTEGRATION") != "1", reason="explicit real ClickHouse RUM gate"
)


def fact(
    ts: str,
    *,
    request_id: str | None = None,
    client_id: str = "fixture-client",
    service_id: str = "TestRum",
    state: str = "visible",
    **fields: Any,
) -> dict[str, Any]:
    return {
        "service_id": service_id,
        "event_id": str(uuid4()),
        "event_timestamp": ts,
        "ingest_timestamp": ts,
        "source_object_key": "raw/fixture.gz",
        "source_object_version": "fixture",
        "line_ordinal": 0,
        "transform_version": "fixture",
        "batch_id": str(uuid4()),
        "publication_state": state,
        "client_id": client_id,
        "request_event_id": request_id,
        "pathname": "/",
        "country": "US",
        **fields,
    }


def vital(ts: str, metric: str, value: float, **kwargs: Any) -> dict[str, Any]:
    return fact(ts, metric_name=metric, metric_value=value, metric_rating="good", **kwargs)


def error(ts: str, **kwargs: Any) -> dict[str, Any]:
    return fact(ts, error_message="fixture exception", error_file="fixture.js", **kwargs)


@pytest.fixture
def run_analytics() -> Iterator[Callable[..., dict[str, Any]]]:
    clients: list[ClickHouseClient] = []

    def run(
        vitals: list[dict[str, Any]],
        errors: list[dict[str, Any]],
        start: str | None = "2026-09-01T00:00:00Z",
        end: str | None = "2026-09-01T02:00:00Z",
    ) -> dict[str, Any]:
        schema = Path("backend/high_scale/sql/rum_schema.sql").read_text()
        # Preserve all canonical columns/types; only replication requires Keeper.
        schema = re.sub(r"ReplicatedMergeTree\([^;]*?\)", "MergeTree()", schema)
        seed = schema
        for table, rows in (("rum_vitals_facts", vitals), ("rum_error_facts", errors)):
            if rows:
                seed += f"\nINSERT INTO {table} FORMAT JSONEachRow\n"
                seed += "\n".join(json.dumps(row) for row in rows) + "\n;\n"

        def transport(request: httpx.Request) -> httpx.Response:
            form = BytesParser(policy=default).parsebytes(
                f"Content-Type: {request.headers['content-type']}\r\n\r\n".encode() + request.read()
            )
            fields: dict[str, str] = {}
            for part in form.iter_parts():
                name = part.get_param("name", header="content-disposition")
                payload = part.get_payload(decode=True)
                assert isinstance(name, str) and isinstance(payload, bytes)
                fields[name] = payload.decode()
            command = [
                "docker",
                "run",
                "--rm",
                "-i",
                "--entrypoint",
                "clickhouse",
                os.getenv("RUM_CLICKHOUSE_TEST_IMAGE", "clickhouse/clickhouse-server:25.8.4.13"),
                "local",
                "--multiquery",
                "--format=JSON",
                "--date_time_input_format=best_effort",
                "--output_format_json_quote_64bit_integers=0",
            ]
            command += [f"--{name}={value}" for name, value in fields.items() if name.startswith("param_")]
            completed = subprocess.run(
                command,
                input=seed + "\n" + fields["query"] + ";",
                text=True,
                capture_output=True,
                timeout=60,
                check=True,
            )
            return httpx.Response(200, content=completed.stdout.encode())

        settings = ClickHouseConfig(host="localhost", port=8123, database="default", user="default", password="")
        client = ClickHouseClient(settings, transport=httpx.MockTransport(transport))
        clients.append(client)
        watermark = ServingWatermark("TestRum", "request", 0, None, None, None, None, None, True)
        return rum_analytics(HighScaleService("TestRum", client, b"test-secret", watermark), start, end)

    yield run
    for client in clients:
        client.close()


def test_real_sql_distinct_counts_inclusive_bounds_and_missing_buckets(run_analytics) -> None:
    shared_id = str(uuid4())
    ts = "2026-09-01T00:00:00Z"
    result = run_analytics(
        [
            vital(ts, "LCP", 1000, request_id=shared_id),
            vital(ts, "CLS", 0.1, request_id=shared_id),
            vital(ts, "event_click", 1, request_id=shared_id),
            vital(ts, "LCP", 5000, client_id="second"),
            vital("2026-09-01T02:00:00Z", "CLS", 0.2),
            vital("2026-09-01T02:00:00.001Z", "LCP", 9000),  # outside inclusive bound
            vital(ts, "LCP", 99999, state="pending"),
            vital(ts, "LCP", 99999, service_id="TestOther"),
        ],
        [
            error(ts, request_id=shared_id),
            error(ts, request_id=shared_id),  # same beacon twice
        ],
    )
    assert result["no_data"] is False
    assert result["beacon_count"] == 3  # shared request ID deduped across both tables
    assert result["pageview_count"] == 3
    assert result["interaction_count"] == 1
    assert result["error_count"] == 1
    assert result["vitals"]["lcp"]["p75"] == 4.0  # interpolated, not quantileExact
    trends = result["trends"]
    assert trends["lcp"] == [4.0, None, None]
    assert trends["cls"] == [0.1, None, 0.2]
    assert trends["pageviews"] == [2, 0, 1]
    assert trends["interactions"] == [1, 0, 0]
    assert trends["errors"] == [1, 0, 0]
    assert trends["error_rate"] == [33.33, None, 0.0]
    assert result["worst_pages"][0]["views"] == 3
    assert result["worst_pages"][0]["lcp_p75"] == 4.0
    assert result["worst_pages"][0]["error_rate"] == 25.0
    assert result["errors"] == [{"message": "fixture exception", "file": "fixture.js", "line": 0, "col": 0, "count": 2}]
    assert result["environments"] == {"browsers": {}, "os": {}, "devices": {}}


@pytest.mark.parametrize("hours,points", [(48, 49), (49, 3), (24 * 30, 31)])
def test_real_sql_daily_buckets_aggregate_all_hours(run_analytics, hours: int, points: int) -> None:
    from datetime import timedelta

    start = datetime(2026, 9, 1, tzinfo=UTC)
    result = run_analytics(
        [
            vital("2026-09-01T00:00:00Z", "lcp", 1000),
            vital("2026-09-01T23:00:00Z", "lcp", 5000),
        ],
        [error("2026-09-02T00:00:00Z")],
        start.isoformat(),
        (start + timedelta(hours=hours)).isoformat(),
    )
    trends = result["trends"]
    assert all(len(values) == points for values in trends.values())
    assert trends["timestamps"] == sorted(trends["timestamps"])
    assert trends["lcp"][0] == (1.0 if hours == 48 else 4.0)
    assert sum(trends["pageviews"]) == 2
    assert sum(trends["errors"]) == 1
    errors_only_bucket = 24 if hours == 48 else 1
    assert trends["lcp"][errors_only_bucket] is None
    assert trends["cls"][errors_only_bucket] is None
    assert trends["error_rate"][errors_only_bucket] == 100.0


def test_real_sql_errors_only_is_not_onboarding(run_analytics) -> None:
    result = run_analytics([], [error("2026-09-01T01:00:00Z")])
    assert result["no_data"] is False
    assert result["beacon_count"] == result["error_count"] == 1
    assert result["pageview_count"] == result["interaction_count"] == 0
    assert result["trends"]["lcp"] == result["trends"]["cls"] == [None, None, None]
    assert result["trends"]["error_rate"] == [None, 100.0, None]
    assert result["errors"]
    assert result["worst_pages"] == []


@pytest.mark.parametrize("older_data", [False, True])
def test_real_sql_empty_range_and_never_configured(run_analytics, older_data: bool) -> None:
    result = run_analytics([vital("2026-08-01T00:00:00Z", "LCP", 1000)] if older_data else [], [])
    assert result["no_data"] is not older_data
    assert result["beacon_count"] == 0
    assert result["trends"]["lcp"] == ([None] * 3 if older_data else [])
    assert result["trends"]["pageviews"] == ([0] * 3 if older_data else [])
    assert result["vitals"]["lcp"]["p75"] is None
    assert result["worst_pages"] == result["errors"] == []


def test_real_sql_utc_offsets_and_partial_hour_edges(run_analytics) -> None:
    result = run_analytics(
        [
            vital("2026-09-01T00:14:59.999Z", "LCP", 9999),
            vital("2026-09-01T00:15:00Z", "LCP", 2000),
            vital("2026-09-01T01:15:00Z", "LCP", 3000),
            vital("2026-09-01T01:15:00.001Z", "LCP", 9999),
        ],
        [],
        "2026-08-31T18:15:00-06:00",
        "2026-08-31T19:15:00-06:00",
    )
    assert result["trends"]["timestamps"] == ["2026-09-01T00:00:00+00:00", "2026-09-01T01:00:00+00:00"]
    assert result["trends"]["lcp"] == [2.0, 3.0]
    assert result["beacon_count"] == 2


def test_real_sql_metric_formats_zero_and_case_normalization(run_analytics) -> None:
    ts = "2026-09-01T00:00:00Z"
    shared = str(uuid4())
    result = run_analytics(
        [
            vital(ts, "LCP", 1000, request_id=shared),
            vital(ts, "lcp", 5000, request_id=shared),
            vital(ts, "CLS", 0, request_id=shared),
            vital(ts, "INP", 123.9, request_id=shared),
            vital(ts, "FID", 12.34, request_id=shared),
            vital(ts, "FCP", 1500, request_id=shared),
            vital(ts, "TTFB", 0.5, request_id=shared),
        ],
        [],
    )
    vitals = result["vitals"]
    assert vitals["lcp"]["p75"] == result["trends"]["lcp"][0] == 4.0
    assert vitals["cls"]["p75"] == result["trends"]["cls"][0] == 0.0
    assert vitals["inp"]["p75"] == 123
    assert vitals["fid"] == {"p75": 12.3, "fcp": 1.5, "ttfb": 0.5}
    assert vitals["lcp"]["distribution"] == {"good": 100, "needs_improvement": 0, "poor": 0}
    assert result["beacon_count"] == result["pageview_count"] == 1


def test_real_sql_interactions_only_keep_measurements_null(run_analytics) -> None:
    result = run_analytics([vital("2026-09-01T01:00:00Z", "event_click", 0)], [])
    assert result["beacon_count"] == result["interaction_count"] == 1
    assert result["pageview_count"] == result["error_count"] == 0
    assert result["trends"]["lcp"] == result["trends"]["cls"] == [None, None, None]
    assert result["trends"]["error_rate"] == [None, 0.0, None]


def test_real_sql_empty_request_ids_use_client_timestamp_identity(run_analytics) -> None:
    ts = "2026-09-01T00:00:00Z"
    result = run_analytics(
        [
            vital(ts, "LCP", 1000),
            vital(ts, "CLS", 0.1),
        ],
        [error(ts)],
    )
    assert result["beacon_count"] == 1
    assert result["pageview_count"] == 1
    assert result["error_count"] == 1
    assert result["worst_pages"][0]["error_rate"] == 50.0
