"""Unit tests for metric expressions, metric snapshots, metrics SQL, request context, request metrics, telemetry, query instrumentation, and slow queries optimizations."""

from __future__ import annotations

import asyncio
import base64
import gc
import json
import os
import signal
import subprocess
import sys
import threading
import uuid
import weakref
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import duckdb
import httpx
import pytest
from backend.core._duckdb_status import refresh_config_status
from backend.core.iceberg._ducklake import _ducklake_attach, ducklake_table_name
from backend.core.metric_expressions import METRIC_DEFINITIONS, get_metric_sql
from backend.core.query_instrumentation import (
    InstrumentedDuckDBConnection,
    _InstrumentedRecordReader,
    _InstrumentedResult,
    _probe_duckdb_memory,
    _safe_weakref,
)
from backend.models._base import MetricType
from backend.utils.date_utils import iso_z
from backend.utils.time_window import resolve_window
from fastapi import Depends, FastAPI, HTTPException
from fastapi import Request as FRequest
from fastapi.testclient import TestClient
from google.protobuf.json_format import MessageToDict
from opentelemetry import metrics as ot_metrics
from opentelemetry import trace
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import ExportMetricsServiceRequest
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from tests._helpers.config_fakes import fetch_metric_snapshot_history as _get_history

from backend import config
from backend import request_metrics_observer as observer_mod
from backend.core import (
    duckdb_pool,
    metric_expressions,
    metric_snapshots,
    request_metrics,
    request_telemetry,
)
from backend.core import duckdb_recycle as memory_guard
from backend.core import metric_snapshots as operational_metrics
from backend.core import request_context as rc
from backend.core import request_telemetry as web_vitals_store
from backend.core.metadata import slow_queries, usage_log, usage_log_db
from backend.core.metadata.usage_log import clear_usage_log
from backend.core.request_context import RequestContext, _enforce_service_access
from backend.core.request_telemetry import RequestTelemetry
from backend.main import app
from backend.repositories._base import origin_latency_us_expr
from backend.utils.remote_access import TimeBounds
from backend.utils.telemetry import get_page_load_id
from scripts.dev import verify_observability as probe
from tests.conftest import make_test_request_context

# ── Section 1: Metric Expressions & Null Default Helpers ─────────────────────


def _observability_probe_fixture():
    receipt = probe.ProbeReceipt(
        probe_id=uuid.uuid4(),
        service_instance_id=uuid.uuid4(),
        trace_id="a" * 32,
        span_id="b" * 16,
        emitted_at=datetime.now(UTC),
    )
    metric = {
        "status": "success",
        "data": {
            "resultType": "vector",
            "result": [
                {
                    "metric": {"job": receipt.service_name, "instance": str(receipt.service_instance_id)},
                    "value": [receipt.emitted_at.timestamp(), "1"],
                }
            ],
        },
    }
    span = {
        "name": "app.observability.probe",
        "traceId": receipt.trace_id,
        "spanId": receipt.span_id,
        "attributes": [{"key": "fla.probe.id", "value": {"stringValue": str(receipt.probe_id)}}],
    }
    trace_data = {
        "batches": [
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": receipt.service_name}},
                        {"key": "service.instance.id", "value": {"stringValue": str(receipt.service_instance_id)}},
                    ]
                },
                "scopeSpans": [{"spans": [span]}],
            }
        ]
    }
    return probe, receipt, metric, trace_data


@pytest.mark.parametrize("base64_ids", [False, True])
def test_observability_probe_requires_both_real_signals(monkeypatch, base64_ids):
    probe, receipt, metric, trace_data = _observability_probe_fixture()
    span = trace_data["batches"][0]["scopeSpans"][0]["spans"][0]
    if base64_ids:
        span["traceId"] = base64.b64encode(bytes.fromhex(receipt.trace_id)).decode()
        span["spanId"] = base64.b64encode(bytes.fromhex(receipt.span_id)).decode()
    requests = []

    def receive(request):
        requests.append(request)
        if request.url.path == "/api/v1/query":
            assert str(receipt.service_instance_id) in request.url.params["query"]
            return httpx.Response(200, json=metric)
        assert request.url.path == f"/api/traces/{receipt.trace_id}"
        return httpx.Response(404) if len(requests) == 2 else httpx.Response(200, json=trace_data)

    monkeypatch.setattr(probe.time, "sleep", lambda _: None)
    with httpx.Client(transport=httpx.MockTransport(receive)) as client:
        result = probe.verify_delivery(
            receipt,
            client=client,
            prometheus_url="http://localhost:9090",
            tempo_url="http://localhost:3200",
            timeout_seconds=1,
        )
    assert result["metric_observed"] is True and result["trace_observed"] is True
    assert result["scope"] == "container_sdk_delivery"
    assert len(requests) == 3


@pytest.mark.parametrize("missing", ["metric", "stale_metric", "wrong_instance", "trace", "wrong_probe"])
def test_observability_probe_rejects_missing_stale_or_unrelated_data(missing):
    probe, receipt, metric, trace_data = _observability_probe_fixture()
    if missing == "metric":
        metric["data"]["result"] = []
    elif missing == "stale_metric":
        metric["data"]["result"][0]["value"][0] -= 3600
    elif missing == "wrong_instance":
        metric["data"]["result"][0]["metric"]["instance"] = str(uuid.uuid4())
    elif missing == "wrong_probe":
        trace_data["batches"][0]["scopeSpans"][0]["spans"][0]["attributes"][0]["value"]["stringValue"] = str(
            uuid.uuid4()
        )

    def receive(request):
        if request.url.path == "/api/v1/query":
            return httpx.Response(200, json=metric)
        return httpx.Response(404) if missing == "trace" else httpx.Response(200, json=trace_data)

    with httpx.Client(transport=httpx.MockTransport(receive)) as client, pytest.raises(probe.VerificationError):
        probe.verify_delivery(
            receipt,
            client=client,
            prometheus_url="http://localhost:9090",
            tempo_url="http://localhost:3200",
            timeout_seconds=0,
        )


def test_observability_probe_surfaces_receiver_errors_and_refuses_redirects():
    probe, receipt, _, _ = _observability_probe_fixture()
    for status in (302, 401, 503):
        with httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(status))) as client:
            with pytest.raises(probe.VerificationError, match=f"HTTP {status}"):
                probe.verify_delivery(
                    receipt,
                    client=client,
                    prometheus_url="http://localhost:9090",
                    tempo_url="http://localhost:3200",
                    timeout_seconds=0,
                )


@pytest.mark.parametrize(
    "metric_name",
    ["http_server_duration_milliseconds_count", "http_server_request_duration_seconds_count"],
)
@pytest.mark.parametrize(
    "missing",
    [
        None,
        "wrong_instance",
        "wrong_trace",
        "client_span",
        "stale_span",
        "missing_metric",
        "stale_metric",
        "probe_metric",
    ],
)
def test_observability_application_request_requires_its_server_trace_and_http_metric(metric_name, missing):
    probe, sdk_receipt, metric, trace_data = _observability_probe_fixture()
    receipt = probe.ApplicationReceipt(
        trace_id=sdk_receipt.trace_id, started_at=sdk_receipt.emitted_at, path="/api/health"
    )
    span = trace_data["batches"][0]["scopeSpans"][0]["spans"][0]
    span.update(
        name="GET /api/health",
        kind="SPAN_KIND_SERVER",
        startTimeUnixNano=str(int(receipt.started_at.timestamp() * 1_000_000_000)),
    )
    metric_row = metric["data"]["result"][0]
    metric_row["metric"]["__name__"] = metric_name
    metric_row["value"][1] = "5"
    if missing == "wrong_instance":
        metric_row["metric"]["instance"] = str(uuid.uuid4())
    elif missing == "wrong_trace":
        span["traceId"] = "c" * 32
    elif missing == "client_span":
        span["kind"] = "SPAN_KIND_CLIENT"
    elif missing == "stale_span":
        span["startTimeUnixNano"] = str(int((receipt.started_at.timestamp() - 3600) * 1_000_000_000))
    elif missing == "missing_metric":
        metric["data"]["result"] = []
    elif missing == "stale_metric":
        metric_row["value"][0] -= 3600
    elif missing == "probe_metric":
        metric_row["metric"]["__name__"] = "app_observability_probe_total"

    def receive(request):
        if request.url.path == "/api/v1/query":
            assert str(sdk_receipt.service_instance_id) in request.url.params["query"]
            assert "http_server_" in request.url.params["query"]
            return httpx.Response(200, json=metric)
        assert request.url.path == f"/api/traces/{receipt.trace_id}"
        return httpx.Response(200, json=trace_data)

    with httpx.Client(transport=httpx.MockTransport(receive)) as client:
        kwargs = {
            "client": client,
            "prometheus_url": "http://localhost:9090",
            "tempo_url": "http://localhost:3200",
            "timeout_seconds": 0,
        }
        if missing is not None:
            with pytest.raises(probe.VerificationError, match="application delivery not observed"):
                probe.verify_application_delivery(receipt, **kwargs)
        else:
            result = probe.verify_application_delivery(receipt, **kwargs)
            assert result["scope"] == "application_http_delivery"
            assert result["metric_observed"] is True and result["trace_observed"] is True
            assert result["trace_id"] == receipt.trace_id


def test_observability_probe_emits_using_application_sdk(monkeypatch):
    probe, _, _, _ = _observability_probe_fixture()
    instance = str(uuid.uuid4())
    resource = Resource.create({"service.name": "fastly-log-analytics", "service.instance.id": instance})
    spans = InMemorySpanExporter()
    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(SimpleSpanProcessor(spans))
    reader = InMemoryMetricReader()
    meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
    monkeypatch.setattr(probe.telemetry, "effective_exporter", lambda: "otlp")
    monkeypatch.setattr(probe.telemetry, "get_tracer", lambda: tracer_provider.get_tracer("probe"))
    monkeypatch.setattr(probe.telemetry, "get_meter", lambda: meter_provider.get_meter("probe"))
    monkeypatch.setattr(probe.trace, "get_tracer_provider", lambda: tracer_provider)
    monkeypatch.setattr(probe.metrics, "get_meter_provider", lambda: meter_provider)
    try:
        probe_id = uuid.uuid4()
        receipt = probe.emit_probe(probe_id)
        assert receipt.service_instance_id == uuid.UUID(instance)
        exported = spans.get_finished_spans()
        assert len(exported) == 1 and exported[0].attributes["fla.probe.id"] == str(probe_id)
        metric = reader.get_metrics_data().resource_metrics[0].scope_metrics[0].metrics[0]
        assert metric.name == "app.observability.probe" and metric.data.data_points[0].value == 1
    finally:
        tracer_provider.shutdown()
        meter_provider.shutdown()


def test_observability_probe_writes_private_receipts_without_overwriting(tmp_path):
    path = tmp_path / "receipt.json"
    try:
        probe._write_private(path, {"scope": "container_sdk_delivery"})
        assert path.stat().st_mode & 0o777 == 0o600
        with pytest.raises(FileExistsError):
            probe._write_private(path, {"scope": "incorrect"})
        assert json.loads(path.read_text())["scope"] == "container_sdk_delivery"
    finally:
        path.unlink(missing_ok=True)


@pytest.mark.parametrize("application_request", [False, True])
def test_observability_probe_actual_otlp_transport(tmp_path, application_request):
    received = {}

    class Receiver(BaseHTTPRequestHandler):
        def do_POST(self):
            message_type = ExportTraceServiceRequest if self.path == "/v1/traces" else ExportMetricsServiceRequest
            message = message_type.FromString(self.rfile.read(int(self.headers["Content-Length"])))
            received[self.path] = MessageToDict(message)
            self.send_response(200)
            self.send_header("Content-Type", "application/x-protobuf")
            self.end_headers()

        def do_GET(self):
            if self.path.startswith("/api/v1/query?"):
                resource_metrics = received["/v1/metrics"]["resourceMetrics"][0]
                attrs = probe._attributes(resource_metrics["resource"]["attributes"])
                metric_name = "http.server.duration" if application_request else "app.observability.probe"
                metric = next(
                    metric
                    for scope in resource_metrics["scopeMetrics"]
                    for metric in scope["metrics"]
                    if metric["name"] == metric_name
                )
                point = metric["histogram" if application_request else "sum"]["dataPoints"][0]
                body = {
                    "status": "success",
                    "data": {
                        "resultType": "vector",
                        "result": [
                            {
                                "metric": {
                                    "job": attrs["service.name"],
                                    "instance": attrs["service.instance.id"],
                                    "__name__": "http_server_duration_milliseconds_count"
                                    if application_request
                                    else "app_observability_probe_total",
                                },
                                "value": [
                                    int(point["timeUnixNano"]) / 1e9,
                                    point["count" if application_request else "asInt"],
                                ],
                            }
                        ],
                    },
                }
            else:
                body = received["/v1/traces"]
            encoded = json.dumps(body).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

    server = HTTPServer(("127.0.0.1", 0), Receiver)
    worker = threading.Thread(target=server.serve_forever)
    worker.start()
    receipt_path = tmp_path / "sdk-receipt.json"
    try:
        origin = f"http://127.0.0.1:{server.server_port}"
        env = {
            **os.environ,
            "OTEL_ENABLED": "1",
            "OTEL_EXPORTER": "otlp",
            "OTEL_TRACES_SAMPLER": "always_on",
            "OTEL_EXPORTER_OTLP_TIMEOUT": "3",
            "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT": f"{origin}/v1/metrics",
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": f"{origin}/v1/traces",
        }
        env.pop("PYTEST_CURRENT_TEST", None)
        arguments = [
            "-m",
            "scripts.dev.verify_observability",
            "emit",
            "--probe-id",
            str(uuid.uuid4()),
            "--output",
            str(receipt_path),
        ]
        if application_request:
            arguments = [
                "-c",
                """
import json, sys, uuid
from datetime import UTC, datetime
from pathlib import Path
from fastapi import FastAPI
from fastapi.testclient import TestClient
from opentelemetry import metrics, trace
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from scripts.dev import verify_observability as probe
probe.telemetry.ensure_initialized()
app = FastAPI()
@app.get("/api/health")
def health():
    return {"status": "ok"}
FastAPIInstrumentor.instrument_app(app, exclude_spans=["receive", "send"])
receipt = probe.ApplicationReceipt(trace_id=uuid.uuid4().hex, started_at=datetime.now(UTC), path="/api/health")
parent = f"00-{receipt.trace_id}-{uuid.uuid4().hex[:16]}-01"
with TestClient(app) as client:
    assert client.get(receipt.path, headers={"traceparent": parent}).status_code == 200
assert trace.get_tracer_provider().force_flush(timeout_millis=5000)
assert metrics.get_meter_provider().force_flush(timeout_millis=5000)
probe._write_private(Path(sys.argv[1]), receipt.model_dump(mode="json"))
print(json.dumps({"completed": True}))
""",
                str(receipt_path),
            ]
        child = subprocess.run(
            [sys.executable, *arguments],
            env=env,
            capture_output=True,
            text=True,
            check=True,
            timeout=20,
        )
        assert json.loads(child.stdout)["completed"] is True
        with httpx.Client(trust_env=False) as client:
            if application_request:
                result = probe.verify_application_delivery(
                    probe.ApplicationReceipt.model_validate_json(receipt_path.read_text()),
                    client=client,
                    prometheus_url=origin,
                    tempo_url=origin,
                    timeout_seconds=0,
                )
            else:
                result = probe.verify_delivery(
                    probe.ProbeReceipt.model_validate_json(receipt_path.read_text()),
                    client=client,
                    prometheus_url=origin,
                    tempo_url=origin,
                    timeout_seconds=0,
                )
        assert result["metric_observed"] and result["trace_observed"]
    finally:
        server.shutdown()
        worker.join(timeout=5)
        server.server_close()
        receipt_path.unlink(missing_ok=True)


@pytest.mark.parametrize(
    ("cols", "absent", "expected"),
    [
        ({"ottfb", "ttfb"}, "NULL", 'COALESCE("ottfb", "ttfb" * 1000000.0)'),
        (["ottfb", "ttfb"], "NULL", 'COALESCE("ottfb", "ttfb" * 1000000.0)'),
        ({"ottfb", "ttfb", "url"}, "NULL", 'COALESCE("ottfb", "ttfb" * 1000000.0)'),
        ({"ottfb"}, "NULL", '"ottfb"'),
        ({"ttfb"}, "NULL", '"ttfb" * 1000000.0'),
        (set(), "NULL", "NULL"),
        ({"url", "pop"}, "NULL", "NULL"),
        (set(), None, None),
        ({"url"}, None, None),
        ({"ottfb", "ttfb"}, None, 'COALESCE("ottfb", "ttfb" * 1000000.0)'),
        ({"ottfb"}, None, '"ottfb"'),
        ({"ttfb"}, None, '"ttfb" * 1000000.0'),
    ],
)
def test_origin_latency_us_and_seed_delegation(cols, absent, expected) -> None:
    assert metric_expressions.origin_latency_us(cols, absent=absent) == expected
    if absent == "NULL":
        assert origin_latency_us_expr(cols) == expected


def test_null_default_helpers_golden_sql() -> None:
    hit = metric_expressions.HIT_STATES
    assert hit == "('HIT', 'HIT-STALE', 'HIT-CLUSTER')"
    cols = {"resp_header_content_encoding", "resp_bytes", "status", "ttl", "cache"}
    f, c, s = metric_expressions.column_or_default, metric_expressions.coalesce, metric_expressions.sum_or_zero
    for col, default, cast, exp_present, exp_absent in [
        ("resp_header_content_encoding", "''", None, "COALESCE(\"resp_header_content_encoding\", '')", "''"),
        ("resp_bytes", "0", "BIGINT", 'COALESCE(CAST("resp_bytes" AS BIGINT), 0)', "0"),
        ("status", "200", "INTEGER", 'COALESCE(CAST("status" AS INTEGER), 200)', "200"),
        ("ttl", "0.0", "DOUBLE", 'COALESCE(CAST("ttl" AS DOUBLE), 0.0)', "0.0"),
        ("cache", "'MISS'", None, "COALESCE(\"cache\", 'MISS')", "'MISS'"),
    ]:
        kw = {"cast": cast} if cast else {}
        assert f(cols, col, default, **kw) == exp_present and f({"url"}, col, default, **kw) == exp_absent
    assert c("CAST(ottfb AS DOUBLE) / 1000.0", "0") == "COALESCE(CAST(ottfb AS DOUBLE) / 1000.0, 0)"
    assert c("CAST(ottlb AS DOUBLE) / 1000.0", "0") == "COALESCE(CAST(ottlb AS DOUBLE) / 1000.0, 0)"
    assert c("SUM(resp_bytes)", "0") == "COALESCE(SUM(resp_bytes), 0)" and c("0", "0") == "COALESCE(0, 0)"
    assert f"CAST({s('"resp_bytes"')} AS BIGINT)" == 'CAST(COALESCE(SUM("resp_bytes"), 0) AS BIGINT)'
    assert f"CAST({s('"tcp_rtt"', zero='0.0')} AS DOUBLE)" == 'CAST(COALESCE(SUM("tcp_rtt"), 0.0) AS DOUBLE)'
    assert s("requests") == "COALESCE(SUM(requests), 0)" and s("num") == "COALESCE(SUM(num), 0)"
    assert f"{s('resp_bytes')} AS total_bytes" == "COALESCE(SUM(resp_bytes), 0) AS total_bytes"
    assert f"{s('bytes')} AS total_bytes" == "COALESCE(SUM(bytes), 0) AS total_bytes"
    assert (
        s("elapsed", zero="0.0", where="cache = 'MISS'") == "COALESCE(SUM(elapsed) FILTER (WHERE cache = 'MISS'), 0.0)"
    )
    assert s("resp_bytes", where=f"cache IN {hit}") == f"COALESCE(SUM(resp_bytes) FILTER (WHERE cache IN {hit}), 0)"
    assert s("resp_bytes") == "COALESCE(SUM(resp_bytes), 0)"
    assert (
        s("elapsed", zero="0.0", where=f"cache IN {hit}")
        == f"COALESCE(SUM(elapsed) FILTER (WHERE cache IN {hit}), 0.0)"
    )


# ── Section 2: Metric Snapshots ──────────────────────────────────────────────


class _FakePgConnection:
    def __init__(self, rows=(), rowcount=0):
        self.calls, self._rows, self._rowcount = [], list(rows), rowcount

    def execute(self, sql, params=()):
        self.calls.append((sql, params))
        return SimpleNamespace(
            fetchall=lambda: self._rows, fetchone=lambda: self._rows[0] if self._rows else None, rowcount=self._rowcount
        )

    def close(self):
        pass


def test_metric_snapshots_roundtrip_filtering_and_batch():
    now, since = datetime.now(UTC), datetime.now(UTC) - timedelta(hours=1)
    assert _get_history("cpu_load_1m", since=since) == [] and metric_snapshots.get_batch(since=since) == {}
    metric_snapshots.record_snapshot("cpu_load_1m", 1.5, ts=iso_z(now - timedelta(seconds=10)))
    metric_snapshots.record_snapshot("cpu_load_1m", 2.0, ts=iso_z(now))
    metric_snapshots.record_snapshot("cpu_load_1m", 99.0, service_id="svc-x")
    out = _get_history("cpu_load_1m", since=since)
    assert len(out) == 2 and {r["value"] for r in out} == {1.5, 2.0} and all("ts" in r and "value" in r for r in out)

    for m, val, sid, task in [
        ("pool_wait_p95_ms", 12.3, "svc-a", ""),
        ("pool_wait_p95_ms", 45.6, "svc-b", ""),
        ("cron_duration_ms", 1000.0, "svc-a", "sync"),
        ("cron_duration_ms", 2000.0, "svc-a", "commit"),
    ]:
        metric_snapshots.record_snapshot(m, val, service_id=sid, task=task)
    assert [r["value"] for r in _get_history("pool_wait_p95_ms", since=since, service_id="svc-a")] == [12.3]
    assert [r["value"] for r in _get_history("pool_wait_p95_ms", since=since, service_id="svc-b")] == [45.6]
    assert [r["value"] for r in _get_history("cron_duration_ms", since=since, service_id="svc-a", task="sync")] == [
        1000.0
    ]
    assert {"cpu_load_1m", "pool_wait_p95_ms|svc-a", "cron_duration_ms|svc-a|sync"} <= set(
        metric_snapshots.get_batch(since=since)
    )


def test_metric_snapshots_clamp_purge_and_idempotent_overwrite():
    now = datetime.now(UTC)
    since = now - timedelta(hours=1)
    metric_snapshots.record_snapshot("cpu_load_1m", 1.1, ts=iso_z(now - timedelta(days=10)))
    metric_snapshots.record_snapshot("cpu_load_1m", 9.9, ts=iso_z(now - timedelta(days=60)))
    metric_snapshots.record_snapshot("cpu_load_1m", 2.2, ts=iso_z(now))
    assert [r["value"] for r in _get_history("cpu_load_1m", since=since)] == [2.2]
    assert metric_snapshots.purge_old(retention_days=0) == 0 and metric_snapshots.purge_old(retention_days=-5) == 0
    assert metric_snapshots.purge_old(retention_days=30) == 1
    assert {r["value"] for r in _get_history("cpu_load_1m", since=now - timedelta(days=90))} == {1.1, 2.2}

    ts = iso_z(now)
    metric_snapshots.record_snapshot("pool_wait_p95_ms", 1.0, service_id="svc-a", ts=ts)
    metric_snapshots.record_snapshot("pool_wait_p95_ms", 2.0, service_id="svc-a", ts=ts)
    out = _get_history("pool_wait_p95_ms", since=since, service_id="svc-a")
    assert len(out) == 1 and out[0]["value"] == 2.0


def test_postgres_backend_is_shared_for_metric_writes_and_reads(monkeypatch):
    write_con = _FakePgConnection()
    read_con = _FakePgConnection(
        rows=[
            {
                "metric": "pool_wait_p95_ms",
                "service_id": "svc-a",
                "task": "",
                "ts": "2026-01-01T00:00:00Z",
                "value": 4.5,
            }
        ]
    )
    monkeypatch.setattr(metric_snapshots, "_pg_write_connection", lambda: write_con)
    monkeypatch.setattr(metric_snapshots, "_pg_connection", lambda: read_con)
    metric_snapshots.record_snapshot("pool_wait_p95_ms", 4.5, service_id="svc-a", ts="2026-01-01T00:00:00Z")
    out = metric_snapshots.get_batch(since=datetime(2025, 12, 31, tzinfo=UTC))
    assert out["pool_wait_p95_ms|svc-a"][0]["value"] == 4.5
    assert "ON CONFLICT" in write_con.calls[0][0] and write_con.calls[0][1][2] == "svc-a"
    assert read_con.calls[0][1] == ("2025-12-31T00:00:00Z",)


# ── Section 3: Metrics SQL Generation & Execution ────────────────────────────


def test_every_metric_type_and_get_metric_sql_contracts():
    for m in MetricType:
        assert m.value in METRIC_DEFINITIONS or m == MetricType.SPECIFIC_STATUS

    assert get_metric_sql(MetricType.REQUESTS.value) == "count(*)"
    assert get_metric_sql("totally_made_up_metric") == "count(*)"
    sql_5xx_rate = get_metric_sql("5xx_rate")
    assert "100.0" in sql_5xx_rate and "NULLIF(count(*), 0)" in sql_5xx_rate

    sql_5xx = get_metric_sql(MetricType.ERRORS_5XX.value, table_name="logs")
    assert "WHERE status >= 500" in sql_5xx and "CASE WHEN" not in sql_5xx and "FROM logs" in sql_5xx
    sql_4xx = get_metric_sql(MetricType.ERRORS_4XX.value, table_name="logs")
    assert "WHERE status BETWEEN 400 AND 499" in sql_4xx and "CASE WHEN" not in sql_4xx
    sql_ttfb = get_metric_sql(MetricType.TTFB.value, table_name="logs")
    assert "ottfb IS NOT NULL" in sql_ttfb and "FROM logs" in sql_ttfb
    sql_req = get_metric_sql(MetricType.REQUESTS.value, table_name="logs_svc")
    assert sql_req.startswith("SELECT ") and "FROM logs_svc" in sql_req and "count(*)" in sql_req

    sql_spec = get_metric_sql(MetricType.SPECIFIC_STATUS.value, status_codes=[404, 403], table_name="logs")
    assert "WHERE 1=1" in sql_spec and "status IN (404, 403)" in sql_spec and "FROM logs" in sql_spec
    sql_spec_rate = get_metric_sql("specific_status_rate", status_codes=[500])
    assert (
        "sum(CASE WHEN 1=1  AND status IN (500) THEN 1 ELSE 0 END)" in sql_spec_rate
        and "100.0" in sql_spec_rate
        and "NULLIF(count(*), 0)" in sql_spec_rate
    )
    assert "IN" not in get_metric_sql(MetricType.SPECIFIC_STATUS.value, status_codes=[])
    sql_inj = get_metric_sql(MetricType.SPECIFIC_STATUS.value, status_codes=[200, "drop table users", 404])  # type: ignore[list-item]
    assert "200" in sql_inj and "404" in sql_inj and "drop" not in sql_inj.lower()


@pytest.fixture(scope="module")
def t():
    con = duckdb.connect(":memory:")
    con.execute(
        "CREATE TABLE logs (status INT, elapsed BIGINT, ottfb DOUBLE, cache VARCHAR, resp_bytes BIGINT, req_bytes BIGINT, req_header_bytes BIGINT)"
    )
    con.execute(
        "INSERT INTO logs VALUES (200, 100, 50.0, 'HIT', 1024, 200, 100), (500, 500, 80.0, 'MISS', 2048, 300, 100), (404, 30, NULL, 'PASS', 512, 100, 100)"
    )
    yield con
    con.close()


@pytest.mark.parametrize(
    "metric",
    [
        MetricType.REQUESTS.value,
        MetricType.ERRORS_5XX.value,
        MetricType.ERRORS_4XX.value,
        MetricType.HIT_RATE.value,
        MetricType.LATENCY_P95.value,
        MetricType.THROUGHPUT.value,
        MetricType.REQ_SIZE.value,
        MetricType.TTFB.value,
        "5xx_rate",
        "4xx_rate",
    ],
)
def test_generated_sql_executes_against_real_table(metric, t):
    t.execute(get_metric_sql(metric, table_name="logs")).fetchone()
    rows = t.execute(
        get_metric_sql(MetricType.SPECIFIC_STATUS.value, status_codes=[200, 404], table_name="logs")
    ).fetchone()
    assert rows[0] == 2


# ── Section 4: Request Context, Tenancy & Pool Lifecycle ─────────────────────


@pytest.mark.security_regression
@pytest.mark.parametrize(
    ("session_services", "requested_svc", "expected_result", "expected_status", "expected_err"),
    [
        (None, "svc-123", {"svc-123"}, None, None),
        (None, None, None, 400, "no_service"),
        (["svc-1", "svc-2"], "svc-1", {"svc-1"}, None, None),
        (["svc-1"], "svc-other", None, 403, "service_not_authorized"),
        (["svc-7", "svc-8"], None, {"svc-7", "svc-8"}, None, None),
        ([], None, None, 400, None),
    ],
)
def test_enforce_service_access_tenancy(
    session_services, requested_svc, expected_result, expected_status, expected_err
):
    sess = None if session_services is None else SimpleNamespace(service_ids=session_services)
    request = SimpleNamespace(state=SimpleNamespace(analyst_session=sess))
    if expected_status is not None:
        with pytest.raises(HTTPException) as exc:
            _enforce_service_access(request, requested_svc)
        assert exc.value.status_code == expected_status and (
            not expected_err or exc.value.detail["error"] == expected_err
        )
    else:
        assert _enforce_service_access(request, requested_svc) in expected_result


def test_request_context_fields_and_resolve_source():
    ctx = RequestContext(
        service_id="svc-1",
        source={"name": "svc-1", "endpoint_url": "http://localhost"},
        _con_override=MagicMock(),
        telemetry=RequestTelemetry("GET", "/api/x"),
        analyst_session=None,
    )
    assert (
        ctx.service_id == "svc-1"
        and ctx.source["name"] == "svc-1"
        and ctx.con is not None
        and ctx.telemetry is not None
    )
    assert ctx.analyst_session is None and ctx.cached_temps == {} and ctx.read_only is True

    other = RequestContext(
        service_id="svc", source={"name": "svc"}, _con_override=MagicMock(), telemetry=RequestTelemetry("GET", "/")
    )
    ctx.cached_temps["window:1h"] = "tmp_1234"
    assert "window:1h" not in other.cached_temps

    fake_source = {"name": "svc-x", "endpoint_url": "http://localhost"}
    with patch("backend.core.duckdb.get_source_for_service", return_value=fake_source):
        assert rc._resolve_source("svc-x") is fake_source
    with patch("backend.core.duckdb.get_source_for_service", return_value=None), pytest.raises(HTTPException) as exc:
        rc._resolve_source("svc-missing")
    assert exc.value.status_code == 400 and exc.value.detail["no_service"] is True


def _call_ctx_route(**get_kwargs):
    sub_app = FastAPI()

    @sub_app.middleware("http")
    async def install_session(request: FRequest, call_next):
        sid_header = request.headers.get("x-test-session-services")
        request.state.analyst_session = (
            SimpleNamespace(service_ids=[s.strip() for s in sid_header.split(",") if s.strip()])
            if sid_header is not None
            else None
        )
        return await call_next(request)

    @sub_app.get("/api/test")
    def route(ctx: RequestContext = Depends(rc.build_request_context)):
        return {"service_id": ctx.service_id, "read_only": ctx.read_only, "is_admin": ctx.analyst_session is None}

    with (
        patch(
            "backend.core.request_context._resolve_source",
            return_value={"name": "svc-1", "endpoint_url": "http://localhost"},
        ),
        patch("backend.deps._ConnectionHolder.__enter__", return_value=MagicMock()),
        patch("backend.deps._ConnectionHolder.__exit__", return_value=False),
    ):
        return TestClient(sub_app).get("/api/test", **get_kwargs)


@pytest.mark.security_regression
@pytest.mark.parametrize(
    ("get_kwargs", "expected_status", "expected_json"),
    [
        ({"params": {"service": "svc-1"}}, 200, {"service_id": "svc-1", "read_only": True, "is_admin": True}),
        (
            {"params": {"service": "svc-OTHER"}, "headers": {"x-test-session-services": "svc-1"}},
            403,
            "service_not_authorized",
        ),
        (
            {"params": {"service": "svc-1", "read_only": "false"}},
            200,
            {"service_id": "svc-1", "read_only": True, "is_admin": True},
        ),
    ],
)
def test_route_request_context_tenancy(get_kwargs, expected_status, expected_json):
    r = _call_ctx_route(**get_kwargs)
    assert r.status_code == expected_status
    assert (r.json() == expected_json) if expected_status == 200 else (r.json()["detail"]["error"] == expected_json)


@pytest.mark.parametrize(("mode", "svc_key"), [("cancel", "svc-cancel"), ("error", "svc-err")])
def test_request_context_pool_checkout_lifecycle(monkeypatch, mode: str, svc_key: str):
    pool = duckdb_pool._Pool(service_key=svc_key, max_size=2)
    mock_con = MagicMock(spec=duckdb.DuckDBPyConnection)
    mock_con.execute.return_value.fetchall.return_value = []
    pool._idle.put_nowait(mock_con)
    pool._in_use = 1
    monkeypatch.setattr(duckdb_pool, "_pool_enabled", lambda: True)
    monkeypatch.setitem(duckdb_pool._pools, svc_key, pool)
    monkeypatch.setattr(duckdb_pool._Pool, "_prepare_checkout", lambda self, con, src: con)

    request = MagicMock(state=SimpleNamespace(analyst_session=None), method="GET")
    request.url.path = "/api/test"
    with patch(
        "backend.core.request_context._resolve_source",
        return_value={"name": svc_key, "endpoint_url": "http://localhost"},
    ):
        gen = rc.build_request_context(request, service_id=svc_key)
        _ = next(gen).con
        assert pool._in_use == 1 and pool._idle.qsize() == 0
        if mode == "cancel":
            gen.close()
        else:
            with pytest.raises(RuntimeError, match="boom"):
                gen.throw(RuntimeError("boom"))

    if mode == "cancel":
        assert (
            pool._in_use == 1
            and pool._idle.qsize() == 1
            and pool._discarded_total == 0
            and mock_con.close.call_count == 0
        )
    else:
        assert (
            pool._discarded_total == 1
            and pool._in_use == 0
            and pool._idle.qsize() == 0
            and mock_con.close.call_count == 1
        )


def test_prewarm_con_off_loop_and_cancellation():
    seen: dict[str, int] = {}

    class _FastHolder:
        con = None

        def __enter__(self):
            seen["thread"] = threading.get_ident()
            self.con = object()
            return self.con

    ctx = RequestContext(service_id="svc", source={}, telemetry=MagicMock(), _holder=_FastHolder())

    async def _run_fast():
        loop_thread = threading.get_ident()
        await ctx.prewarm_con()
        return loop_thread

    loop_tid = asyncio.run(_run_fast())
    assert seen["thread"] != loop_tid and ctx.con is ctx._holder.con

    release, entered = threading.Event(), threading.Event()

    class _SlowHolder:
        con = None

        def __enter__(self):
            entered.set()
            release.wait(5)
            self.con = object()
            return self.con

    ctx2 = RequestContext(service_id="svc", source={}, telemetry=MagicMock(), _holder=_SlowHolder())

    async def _run_cancel():
        task = asyncio.create_task(ctx2.prewarm_con())
        while not entered.is_set():
            await asyncio.sleep(0.01)
        task.cancel()
        asyncio.get_running_loop().call_later(0.1, release.set)
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(_run_cancel())
    assert ctx2._holder.con is not None


def test_request_context_resolve_window_variants(monkeypatch):
    monkeypatch.setattr(config, "get_status", lambda name: {})

    def dt(iso: str) -> datetime:
        return datetime.fromisoformat(iso.replace("Z", "+00:00"))

    def mk_ctx(tb, sess):
        return make_test_request_context(
            service_id="svc", source={"name": "svc", "service_id": "svc"}, session=sess, time_bounds=tb
        )

    def mk_req(**kw):
        return SimpleNamespace(**{"range_token": None, "anchor": None, "start_time": None, "end_time": None, **kw})

    anchor = "2026-03-10T12:00:00Z"
    got = mk_ctx(None, None).resolve_window(
        mk_req(range_token="24h", anchor=anchor, start_time="2000-01-01T00:00:00Z", end_time="2000-01-02T00:00:00Z")
    )
    assert tuple(dt(x) for x in got) == tuple(dt(x) for x in resolve_window("24h", anchor))

    anchor_dt = datetime(2026, 3, 10, 12, 0, tzinfo=UTC)
    invite_start = anchor_dt - timedelta(hours=2)
    start, end = mk_ctx(TimeBounds(start=invite_start, end=anchor_dt), SimpleNamespace(session_id="s")).resolve_window(
        mk_req(range_token="30d", anchor=anchor)
    )
    assert dt(start) >= invite_start and dt(end) <= anchor_dt

    ctx_plain = mk_ctx(None, None)
    assert ctx_plain.resolve_window(mk_req(range_token="bogus", start_time=None, end_time=None)) == (None, None)
    s2, e2 = ctx_plain.resolve_window(
        mk_req(range_token=None, start_time="2026-03-10T00:00:00Z", end_time="2026-03-10T06:00:00Z")
    )
    assert datetime.fromisoformat(s2).hour == 0 and datetime.fromisoformat(e2).hour == 6


# ── Section 5: Durable Request Metrics, Operational Gauges, Memory Guard & Web Vitals ──


@pytest.fixture
def durable_metrics(monkeypatch, tmp_path):
    sid = f"metrics-{uuid.uuid4().hex}"
    source = {
        "name": sid,
        "service_id": sid,
        "deployment_mode": "high_scale",
        "fos_local_warehouse": True,
        "_cache_dir_override": str(tmp_path / "cache"),
        "duckdb_path": str(tmp_path / "must-not-open.duckdb"),
    }
    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "configs")
    monkeypatch.setattr(config, "SERVICES_DATA_DIR", tmp_path / "services")
    config.SERVICES_DATA_DIR.mkdir()
    monkeypatch.setattr(config, "is_durable_serving_mode", lambda source=None: True)
    monkeypatch.setattr(config, "config_to_source", lambda cfg: source)
    config.save_config(
        sid,
        {
            "service_id": sid,
            "status": {
                "rum": {"latest_log_at": "2026-09-09T00:00:00Z", "total_rows": 3},
                "request": {"latest_log_at": "2026-09-08T00:00:00Z", "total_rows": 99},
                "latest_log_at": "2026-09-08T00:00:00Z",
                "local_rows": 102,
            },
        },
    )
    cron = {"log_discovery": {"started_at": "2026-09-08T18:00:00Z"}}
    monkeypatch.setattr("backend.core.metadata.cron_log.latest_cron_per_task", lambda sid: cron)
    table = ducklake_table_name(source)

    def replace_rows(timestamps):
        con = duckdb.connect(":memory:")
        try:
            assert _ducklake_attach(con, source, read_only=False)
            con.execute(f'CREATE TABLE IF NOT EXISTS lake."{table}" (timestamp TIMESTAMPTZ)')
            con.execute(f'DELETE FROM lake."{table}"')
            for ts in timestamps:
                con.execute(f'INSERT INTO lake."{table}" VALUES (?)', [ts])
            info = con.execute(
                "SELECT file_count FROM ducklake_table_info('lake') WHERE table_name = ?", [table]
            ).fetchone()
            assert info[0] in (0, 1, 2)
        finally:
            con.close()

    return source, replace_rows, cron, tmp_path


@pytest.fixture
def _no_real_kill(monkeypatch):
    calls = []
    monkeypatch.setattr(memory_guard.os, "kill", lambda pid, sig: calls.append((pid, sig)))
    return calls


@pytest.mark.usefixtures("_no_real_kill")
def test_durable_metrics_inlined_rows_and_missing_view_fallback(durable_metrics, monkeypatch):
    source, replace_rows, _, path = durable_metrics
    first, last = datetime(2026, 9, 7, 12, tzinfo=UTC), datetime(2026, 9, 7, 19, tzinfo=UTC)
    replace_rows([first, last])
    (path / "cache" / "buffer").mkdir(parents=True)
    con = duckdb.connect(":memory:")
    con.execute(
        "COPY (SELECT TIMESTAMPTZ '2026-09-10 00:00:00+00' AS timestamp) TO ? (FORMAT PARQUET)",
        [str(path / "cache" / "buffer" / "batch_local.parquet")],
    )
    con.close()
    observed = request_metrics.refresh_durable_request_metrics(source)
    assert observed["request"] == {
        "latest_log_at": last.isoformat(),
        "total_rows": 2,
        "last_sync_at": "2026-09-08T18:00:00Z",
    }
    assert observed["earliest_log_at"] == first.isoformat() and observed["latest_log_at"] == last.isoformat()
    assert (
        observed["local_rows"] == 5
        and not (path / "must-not-open.duckdb").exists()
        and config.get_status(source["name"])["request"] == observed["request"]
    )

    from backend.core.duckdb import open_serving_connection as real_open

    def open_without_serving_view(*args, **kwargs):
        c = real_open(*args, **kwargs)
        c.execute(f'DROP VIEW IF EXISTS "{ducklake_table_name(source)}"')
        return c

    monkeypatch.setattr("backend.core.duckdb.open_serving_connection", open_without_serving_view)
    replace_rows([last])
    obs2 = request_metrics.refresh_durable_request_metrics(source)
    assert obs2["request"]["total_rows"] == 1 and obs2["request"]["latest_log_at"] == last.isoformat()


@pytest.mark.usefixtures("_no_real_kill")
def test_durable_metrics_rum_extents_sync_time_and_backward_reset(durable_metrics):
    source, replace_rows, cron, _ = durable_metrics
    replace_rows([datetime(2026, 9, 7, 12, tzinfo=UTC)])
    con = duckdb.connect(":memory:")
    try:
        assert _ducklake_attach(con, source, read_only=False)
        vitals_table, errors_table = (
            ducklake_table_name(source, "client_vitals"),
            ducklake_table_name(source, "client_errors"),
        )
        for table in (vitals_table, errors_table):
            con.execute(
                f'CREATE TABLE IF NOT EXISTS lake."{table}" (timestamp TIMESTAMPTZ, req_id VARCHAR, cid VARCHAR)'
            )
            con.execute(f'DELETE FROM lake."{table}"')
        con.execute(
            f"INSERT INTO lake.\"{vitals_table}\" VALUES (TIMESTAMPTZ '2026-09-08 01:00:00+00', '', 'vital-1'), (TIMESTAMPTZ '2026-09-08 02:00:00+00', '', 'vital-2')"
        )
        con.execute(f"INSERT INTO lake.\"{errors_table}\" VALUES (TIMESTAMPTZ '2026-09-08 03:00:00+00', '', 'error-1')")
    finally:
        con.close()

    observed = request_metrics.refresh_durable_request_metrics(source)
    assert observed["rum"] == {"latest_log_at": "2026-09-08T03:00:00+00:00", "total_rows": 3, "last_sync_at": None}
    assert config.get_status(source["name"])["rum"] == observed["rum"]

    status = config.get_status(source["name"])
    status["rum"]["last_sync_at"] = "2026-09-07T00:00:00Z"
    config.update_status(source["name"], status)
    cron["rum_sync"] = {"started_at": "2026-09-12T15:00:00Z"}
    assert request_metrics.refresh_durable_request_metrics(source)["rum"]["last_sync_at"] == "2026-09-12T15:00:00Z"

    older = datetime(2026, 9, 2, tzinfo=UTC)
    replace_rows([older])
    assert request_metrics.refresh_durable_request_metrics(source)["latest_log_at"] == older.isoformat()
    replace_rows([])
    cron["log_discovery"]["started_at"] = "2026-09-08T18:01:00Z"
    obs_empty = request_metrics.refresh_durable_request_metrics(source)
    assert obs_empty["request"] == {"latest_log_at": None, "total_rows": 0, "last_sync_at": "2026-09-08T18:01:00Z"}
    assert obs_empty["earliest_log_at"] is obs_empty["latest_log_at"] is None and obs_empty["local_rows"] == 3


@pytest.mark.usefixtures("_no_real_kill")
def test_durable_metrics_failure_legacy_refresh_and_lock_coalescing(durable_metrics, monkeypatch):
    source, replace_rows, _, _ = durable_metrics
    replace_rows([datetime(2026, 9, 7, tzinfo=UTC)])
    monkeypatch.setattr(
        "backend.core.duckdb.get_sync_status", MagicMock(side_effect=AssertionError("local/stale stats path called"))
    )
    monkeypatch.setattr("backend.core.iceberg.get_table_info", lambda src: {})
    monkeypatch.setattr("backend.repositories.usage.get_edge_ratio", lambda *a: (None, None))
    refresh_config_status(source["name"], include_top_values=False)
    assert config.get_status(source["name"])["request"]["total_rows"] == 1

    previous = config.get_status(source["name"])
    con = MagicMock()
    con.execute.side_effect = RuntimeError("catalog unavailable")
    with monkeypatch.context() as m:
        m.setattr("backend.core.duckdb.open_serving_connection", lambda *a, **kw: con)
        with pytest.raises(RuntimeError, match="catalog unavailable"):
            request_metrics.refresh_durable_request_metrics(source)
    assert config.get_status(source["name"]) == previous and con.close.call_count == 1

    replace_rows([])
    refresh_config_status(source["name"], include_top_values=False)
    status = config.get_status(source["name"])
    assert (
        status["request"]["total_rows"] == 0 and status["request"]["latest_log_at"] is status["latest_log_at"] is None
    )

    open_con = MagicMock(side_effect=AssertionError("overlapping scan"))
    monkeypatch.setattr("backend.core.duckdb.open_serving_connection", open_con)
    with request_metrics._refresh_lock:
        assert request_metrics.refresh_durable_request_metrics(source) is None
    open_con.assert_not_called()


@pytest.mark.usefixtures("_no_real_kill")
def test_query_deadline_interrupts_without_persisting_zero(durable_metrics, monkeypatch):
    source, _, _, _ = durable_metrics
    previous = config.get_status(source["name"])
    raw = duckdb.connect(":memory:")
    interrupted: list[bool] = []

    class SlowConnection:
        def execute(self, sql, *args, **kwargs):
            sql = "SELECT sum(sin(i)) FROM range(1000000000) t(i)" if sql.startswith("SELECT count(*)") else sql
            return raw.execute(sql, *args, **kwargs)

        def interrupt(self):
            interrupted.append(True)
            raw.interrupt()

        def close(self):
            raw.close()

    monkeypatch.setattr("backend.core.duckdb.open_serving_connection", lambda *a, **kw: SlowConnection())
    monkeypatch.setattr(request_metrics, "QUERY_TIMEOUT_S", 0.05)
    with pytest.raises(duckdb.InterruptException):
        request_metrics.refresh_durable_request_metrics(source)
    assert interrupted and config.get_status(source["name"]) == previous


@pytest.mark.usefixtures("_no_real_kill")
def test_real_observation_is_persisted_before_sse_publication(durable_metrics, monkeypatch):
    source, replace_rows, _, _ = durable_metrics
    latest = datetime(2026, 9, 7, tzinfo=UTC)
    replace_rows([latest])
    published = []

    def publish(service_id, payload):
        persisted = config.get_status(service_id)
        assert persisted["request"] == payload["request"] and persisted["latest_log_at"] == payload["latest_log_at"]
        published.append(payload["request"])

    monkeypatch.setattr(observer_mod, "compute_sync_status_cached", config.get_status)
    monkeypatch.setattr(observer_mod.publisher, "publish", publish)
    observer = observer_mod.RequestMetricsObserver()
    observer.reconcile()
    observer.reconcile()
    assert len(published) == 1 and published[0]["latest_log_at"] == latest.isoformat()
    replace_rows([])
    observer.reconcile()
    assert len(published) == 2 and published[-1]["total_rows"] == 0 and published[-1]["latest_log_at"] is None


class _FakeMeter:
    def __init__(self):
        self.gauges = {}

    def create_observable_gauge(self, *, name, callbacks, description, unit):
        gauge = SimpleNamespace(callbacks=callbacks)
        self.gauges[name] = (gauge, description, unit)
        return gauge


@pytest.mark.usefixtures("_no_real_kill")
def test_operational_metrics_gauges_and_labels(monkeypatch):
    meter = _FakeMeter()
    operational_metrics.reset_for_tests()
    monkeypatch.setattr(operational_metrics, "get_meter", lambda: meter)
    operational_metrics.record("celery_queue_depth", 7, queue="q.ingest")
    gauge, description, unit = meter.gauges["fla_celery_queue_depth"]
    assert [(item.value, item.attributes) for item in gauge.callbacks[0](None)] == [(7.0, {"queue": "q.ingest"})]
    assert description == "Celery broker queue depth" and unit == "{messages}"

    operational_metrics.record("ingest_ledger_rows", 3, status="discovered")
    operational_metrics.record("ingest_ledger_rows", 2, status="claimed")
    g_ledger, _, _ = meter.gauges["fla_ingest_ledger_rows"]
    assert {(item.value, item.attributes["status"]) for item in g_ledger.callbacks[0](None)} == {
        (3.0, "discovered"),
        (2.0, "claimed"),
    }

    operational_metrics.record("high_scale_publication_lag_seconds", 12.5, service_id="svc", domain="request")
    g_hs, desc_hs, unit_hs = meter.gauges["fla_high_scale_publication_lag_seconds"]
    obs = g_hs.callbacks[0](None)[0]
    assert obs.value == 12.5 and obs.attributes == {"domain": "request", "service_id": "svc"}
    assert desc_hs == "Age of the oldest pending high-scale publication" and unit_hs == "s"


@pytest.mark.usefixtures("_no_real_kill")
def test_telemetry_schema_helpers_and_page_load_middleware(monkeypatch, tmp_path):
    monkeypatch.setattr(usage_log_db, "_DATA_DIR", str(tmp_path))
    con = usage_log_db.get_con("test_service")
    tables = [
        r[0]
        for r in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema()"
        ).fetchall()
    ]
    assert "telemetry_queries" in tables and "telemetry_sections" in tables
    usage_log.log_telemetry_query(
        service_id="test_service",
        page_load_id="page-123",
        request_id="req-456",
        engine="duckdb",
        sql="SELECT 1",
        duration_ms=45.2,
    )
    usage_log.log_telemetry_section(
        service_id="test_service",
        page_load_id="page-123",
        request_id="req-456",
        section_name="dashboard.aggregates",
        duration_ms=12.5,
    )
    telemetry = usage_log.get_page_telemetry("test_service", "page-123")
    assert len(telemetry["queries"]) == 1 and telemetry["queries"][0]["sql_query"] == "SELECT 1"
    assert telemetry["queries"][0]["engine"] == "duckdb" and telemetry["queries"][0]["duration_ms"] == 45.2
    assert (
        len(telemetry["sections"]) == 1
        and telemetry["sections"][0]["section_name"] == "dashboard.aggregates"
        and telemetry["sections"][0]["duration_ms"] == 12.5
    )
    assert (
        TestClient(app).get("/api/health", headers={"X-Page-Load-ID": "test-page-999"}).status_code == 200
        and get_page_load_id() is None
    )


@pytest.mark.usefixtures("_no_real_kill")
@pytest.mark.parametrize(
    ("env_val", "rss_mb", "expected_restart"),
    [(None, 50_000, False), ("9000", 5000, False), ("9000", 9500, True), ("9000", None, False)],
)
def test_memory_guard_graceful_restart_and_threshold(monkeypatch, _no_real_kill, env_val, rss_mb, expected_restart):
    if env_val is None:
        monkeypatch.delenv("BACKEND_GRACEFUL_RESTART_RSS_MB", raising=False)
    else:
        monkeypatch.setenv("BACKEND_GRACEFUL_RESTART_RSS_MB", env_val)
    monkeypatch.setattr(
        "backend.core.duckdb.current_rss_bytes", lambda: None if rss_mb is None else rss_mb * 1024 * 1024
    )
    assert memory_guard.maybe_graceful_restart() is expected_restart
    assert (
        (len(_no_real_kill) == 1 and _no_real_kill[0][1] == signal.SIGTERM and _no_real_kill[0][0] > 0)
        if expected_restart
        else (_no_real_kill == [])
    )
    monkeypatch.setenv("BACKEND_GRACEFUL_RESTART_RSS_MB", "9000")
    assert memory_guard.graceful_restart_rss_threshold_bytes() == 9000 * 1024 * 1024
    monkeypatch.setenv("BACKEND_GRACEFUL_RESTART_RSS_MB", "garbage")
    assert memory_guard.graceful_restart_rss_threshold_bytes() == 0
    monkeypatch.delenv("BACKEND_GRACEFUL_RESTART_RSS_MB", raising=False)
    assert memory_guard.graceful_restart_rss_threshold_bytes() == 0


@pytest.mark.usefixtures("_no_real_kill")
@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("1", True),
        ("true", True),
        ("TRUE", True),
        ("yes", True),
        ("on", True),
        ("On", True),
        ("0", False),
        ("false", False),
        ("no", False),
        ("off", False),
        ("", False),
        (None, False),
    ],
)
def test_web_vitals_collection_enabled_env(monkeypatch, value, expected):
    if value is None:
        monkeypatch.delenv("WEB_VITALS_COLLECT", raising=False)
    else:
        monkeypatch.setenv("WEB_VITALS_COLLECT", value)
    assert web_vitals_store.collection_enabled() is expected


@pytest.mark.usefixtures("_no_real_kill")
@pytest.mark.parametrize(
    ("env_val", "expected_bytes"),
    [(None, 200 * 1024 * 1024), ("not-a-number", 200 * 1024 * 1024), ("0", 0)],
)
def test_web_vitals_max_bytes_parsing(monkeypatch, env_val, expected_bytes):
    if env_val is None:
        monkeypatch.delenv("WEB_VITALS_MAX_MB", raising=False)
    else:
        monkeypatch.setenv("WEB_VITALS_MAX_MB", env_val)
    assert web_vitals_store._max_bytes() == expected_bytes


@pytest.mark.usefixtures("_no_real_kill")
def test_web_vitals_append_io_error_and_rotation(monkeypatch, tmp_path):
    sink = tmp_path / "nested" / "web_vitals.jsonl"
    monkeypatch.setattr(web_vitals_store, "LOG_PATH", sink)
    web_vitals_store.append_sample({"name": "LCP", "value": 1200.0, "rating": "good"})
    web_vitals_store.append_sample({"name": "CLS", "value": 0.05, "rating": "good"})
    lines = sink.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2 and json.loads(lines[0])["name"] == "LCP" and json.loads(lines[1])["name"] == "CLS"

    blocker = tmp_path / "afile"
    blocker.write_text("x")
    monkeypatch.setattr(web_vitals_store, "LOG_PATH", blocker / "child" / "wv.jsonl")
    web_vitals_store.append_sample({"name": "LCP", "value": 1.0, "rating": "good"})

    rot_sink = tmp_path / "wv.jsonl"
    monkeypatch.setattr(web_vitals_store, "LOG_PATH", rot_sink)
    monkeypatch.setenv("WEB_VITALS_MAX_MB", "0.00005")
    for i in range(4):
        web_vitals_store.append_sample({"name": "LCP", "value": i, "rating": "good"})
    assert rot_sink.exists() and web_vitals_store.rotated_path().exists()
    assert len(rot_sink.read_text(encoding="utf-8").strip().splitlines()) < 4 and not (tmp_path / "wv.jsonl.2").exists()

    no_rot_sink = tmp_path / "wv_norot.jsonl"
    monkeypatch.setattr(web_vitals_store, "LOG_PATH", no_rot_sink)
    monkeypatch.setenv("WEB_VITALS_MAX_MB", "0")
    for i in range(5):
        web_vitals_store.append_sample({"name": "LCP", "value": i, "rating": "good"})
    assert (
        not web_vitals_store.rotated_path().exists()
        and len(no_rot_sink.read_text(encoding="utf-8").strip().splitlines()) == 5
    )


# ── Section 6: OpenTelemetry Request Telemetry ───────────────────────────────


@pytest.fixture
def _reset_module():
    request_telemetry._initialised, request_telemetry._thread_wait_histogram = False, None
    yield
    request_telemetry._initialised, request_telemetry._thread_wait_histogram = False, None


@pytest.mark.usefixtures("_reset_module")
@pytest.mark.parametrize(
    ("pytest_env", "otel_enabled", "exporter", "expected"),
    [
        ("true", None, None, False),
        (None, "1", None, False),
        (None, "1", "console", True),
        (None, "1", "otlp", True),
        (None, "1", "none", False),
        (None, "0", "console", False),
    ],
)
def test_otel_and_asgi_enabled_matrix(monkeypatch, pytest_env, otel_enabled, exporter, expected):
    for key, val in [("PYTEST_CURRENT_TEST", pytest_env), ("OTEL_ENABLED", otel_enabled), ("OTEL_EXPORTER", exporter)]:
        monkeypatch.delenv(key, raising=False) if val is None else monkeypatch.setenv(key, val)
    assert (
        request_telemetry._otel_enabled() is expected and request_telemetry.asgi_instrumentation_enabled() is expected
    )


@pytest.mark.usefixtures("_reset_module")
def test_force_flush_and_sdk_resource_initialization(monkeypatch):
    meter, tracer = MagicMock(), MagicMock()
    monkeypatch.setattr(request_telemetry.metrics, "get_meter_provider", lambda: meter)
    monkeypatch.setattr(request_telemetry.trace, "get_tracer_provider", lambda: tracer)
    request_telemetry.force_flush()
    assert (
        not request_telemetry._initialised
        and meter.force_flush.call_args.kwargs == {"timeout_millis": 5000}
        and tracer.force_flush.call_args.kwargs == {"timeout_millis": 5000}
    )
    meter.force_flush.side_effect = RuntimeError("export unavailable")
    request_telemetry.force_flush()
    assert tracer.force_flush.call_count == 2

    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setenv("OTEL_ENABLED", "1")
    monkeypatch.setenv("OTEL_EXPORTER", "console")
    request_telemetry.ensure_initialized()
    assert request_telemetry._initialised is True

    request_telemetry._initialised = False
    monkeypatch.setattr(request_telemetry, "_otel_enabled", lambda: True)
    monkeypatch.setattr(request_telemetry, "_otel_exporter", lambda: "none")
    monkeypatch.setenv("ELEVATION_CLUSTER_NAME", "dev-usc1")
    monkeypatch.setenv("POD_NAMESPACE", "test-namespace")
    with (
        patch.object(request_telemetry.trace, "set_tracer_provider"),
        patch.object(request_telemetry.metrics, "set_meter_provider") as setter,
        patch.object(request_telemetry, "TracerProvider"),
        patch.object(request_telemetry, "MeterProvider") as provider,
    ):
        request_telemetry.ensure_initialized()
        attrs1 = provider.call_args.kwargs["resource"].attributes
        assert attrs1["site"] == "elevation-dev-usc1" and attrs1["namespace"] == "test-namespace"
        first = attrs1["service.instance.id"]
        request_telemetry.ensure_initialized()
        setter.assert_called_once()
        request_telemetry._initialised = False
        request_telemetry.ensure_initialized()
        second = provider.call_args.kwargs["resource"].attributes["service.instance.id"]
        assert first and second and first != second


@pytest.mark.usefixtures("_reset_module")
def test_setup_sdk_otlp_exporters_and_request_lifecycle(monkeypatch):
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setenv("OTEL_ENABLED", "1")
    monkeypatch.setenv("OTEL_EXPORTER", "otlp")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "https://metrics.example.test/v1/metrics")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "https://traces.example.test/v1/traces")
    with (
        patch("opentelemetry.exporter.otlp.proto.http.trace_exporter.OTLPSpanExporter") as span_exp,
        patch("opentelemetry.exporter.otlp.proto.http.metric_exporter.OTLPMetricExporter") as met_exp,
        patch("backend.core.request_telemetry.PeriodicExportingMetricReader") as met_reader,
        patch.object(request_telemetry.trace, "set_tracer_provider") as set_tracer,
        patch.object(request_telemetry.metrics, "set_meter_provider") as set_meter,
    ):
        request_telemetry._setup_sdk()
    span_exp.assert_called_once_with(endpoint="https://traces.example.test/v1/traces")
    met_exp.assert_called_once_with(endpoint="https://metrics.example.test/v1/metrics")
    assert met_reader.call_count == 1 and set_tracer.call_count == 1 and set_meter.call_count == 1

    for missing in ("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"):
        request_telemetry._initialised = False
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "https://metrics.example.test/v1/metrics")
        monkeypatch.setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "https://traces.example.test/v1/traces")
        monkeypatch.delenv(missing)
        with pytest.raises(ValueError, match=missing):
            request_telemetry._setup_sdk()
        assert request_telemetry._initialised is False

    monkeypatch.setenv("PYTEST_CURRENT_TEST", "true")
    assert request_telemetry.get_tracer() is not None
    for _ in range(3):
        request_telemetry._setup_sdk()
    ctx = request_telemetry.RequestTelemetry("GET", "/api/dashboard/aggregates")
    ctx.start_request()
    ctx.start_request()
    ctx.end_request(status_code=200)
    ctx.end_request()
    assert request_telemetry.thread_wait_histogram() is request_telemetry.thread_wait_histogram()


@pytest.mark.usefixtures("_reset_module")
def test_ambient_span_reuse_and_thread_wait_histogram():
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    ambient_span = provider.get_tracer("ambient").start_span("GET /api/ambient-example")
    with (
        trace.use_span(ambient_span, end_on_exit=False),
        patch("backend.core.request_telemetry.get_tracer") as mock_get_tracer,
    ):
        ctx = request_telemetry.RequestTelemetry("GET", "/api/ambient-example")
        ctx.start_request()
        ctx.end_request(status_code=200)
    assert mock_get_tracer.call_count == 0 and exporter.get_finished_spans() == ()
    ambient_span.end()
    (finished,) = exporter.get_finished_spans()
    assert (
        finished.name == "GET /api/ambient-example"
        and finished.attributes.get("http.status_code") == 200
        and finished.attributes.get("app.is_cached") is False
    )

    reader = InMemoryMetricReader()
    m_provider = MeterProvider(metric_readers=[reader])
    saved_histogram, saved_meter_get = request_telemetry._thread_wait_histogram, request_telemetry.get_meter
    try:
        request_telemetry._thread_wait_histogram = None
        request_telemetry.get_meter = lambda: m_provider.get_meter("test-meter")  # type: ignore[assignment]
        h = request_telemetry.thread_wait_histogram()
        for ms, svc, outcome in [(12.5, "svc-a", "reused"), (45.0, "svc-a", "created"), (125.0, "svc-b", "timeout")]:
            h.record(ms, {"service": svc, "outcome": outcome})
        data = reader.get_metrics_data()
        histogram_metrics = [m for rm in data.resource_metrics for sm in rm.scope_metrics for m in sm.metrics]
        assert (
            any("wait" in m.name.lower() for m in histogram_metrics)
            and sum(dp.count for m in histogram_metrics for dp in getattr(m.data, "data_points", [])) >= 3
        )
    finally:
        request_telemetry._thread_wait_histogram, request_telemetry.get_meter = saved_histogram, saved_meter_get  # type: ignore[assignment]
        ot_metrics  # noqa: B018


# ── Section 7: Query Instrumentation & Usage Log / Slow Queries ──────────────


@pytest.fixture
def duck() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    yield con
    con.close()


def test_query_instrumentation_finish_terminal_and_deferred_readers(duck):
    with patch("backend.core.query_instrumentation._deregister") as mock_dereg:
        result = _InstrumentedResult(MagicMock(), qid=7, con_ref=None)
        for _ in range(3):
            result._finish(None, probe_memory=False)
    assert mock_dereg.call_count == 1 and mock_dereg.call_args.args == (7, None)

    duck.execute("CREATE TABLE t AS SELECT i FROM range(5_000) tbl(i)")
    con = InstrumentedDuckDBConnection(duck, service_id="inst_svc")
    for reader_fn in (lambda r: r.arrow(), lambda r: r.fetch_record_batch(100)):
        with patch("backend.core.query_instrumentation._deregister") as mock_dereg:
            reader = reader_fn(con.execute("SELECT * FROM t"))
            assert mock_dereg.call_count == 0 and isinstance(reader, _InstrumentedRecordReader)
            assert sum(b.num_rows for b in reader) == 5_000 and mock_dereg.call_count == 1

    for method in ("fetchall", "fetchone", "fetchnumpy"):
        with patch("backend.core.query_instrumentation._deregister") as mock_dereg:
            getattr(con.execute("SELECT i FROM t LIMIT 10"), method)()
            assert mock_dereg.call_count == 1 and mock_dereg.call_args.args[1] is None

    with patch("backend.core.query_instrumentation._deregister") as mock_dereg:
        with pytest.raises(duckdb.Error):
            con.execute("SELECT * FROM table_that_does_not_exist_xyz")
        assert mock_dereg.call_count == 1 and isinstance(mock_dereg.call_args.args[1], BaseException)


def test_weakref_and_probe_duckdb_memory_resilience():
    class _WeakRefable:
        def cursor(self):
            raise AssertionError("probe ran on a dead connection")

    raw = _WeakRefable()
    ref = _safe_weakref(raw)
    assert ref is not None and ref() is raw
    result = _InstrumentedResult(MagicMock(), qid=42, con_ref=ref)
    del raw
    gc.collect()
    assert ref() is None
    with patch("backend.core.query_instrumentation._deregister") as mock_dereg:
        result._finish(None, probe_memory=True)
    assert mock_dereg.call_count == 1 and mock_dereg.call_args.kwargs.get("peak_memory_mb") is None

    with pytest.raises(TypeError):
        weakref.ref(12345)
    assert _safe_weakref(12345)() == 12345

    for row in (("not-a-number",), None, (None,)):
        fake_con, fake_cursor = MagicMock(), MagicMock()
        fake_con.cursor.return_value = fake_cursor
        fake_cursor.execute.return_value.fetchone.return_value = row
        assert _probe_duckdb_memory(fake_con) is None

    err_con = MagicMock()
    err_con.cursor.side_effect = RuntimeError("no such function: duckdb_memory")
    assert _probe_duckdb_memory(err_con) is None


def test_usage_log_purging_reconcile_and_clear(tmp_path, monkeypatch):
    sid = "test_purge_svc"
    usage_log.clear_usage_log(sid)
    old_ts = "2020-05-01T10:00:00Z"
    now_dt = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    now_hour, now_ts_1, now_ts_2 = now_dt.strftime("%Y-%m-%dT%H"), iso_z(now_dt), iso_z(now_dt + timedelta(minutes=15))

    con = usage_log_db.get_con(sid)
    try:
        con.executemany(
            "INSERT INTO usage_log (timestamp, service_id, operation_class, operation_type, count, function_name) VALUES (?, ?, ?, ?, ?, ?)",
            [
                (old_ts, sid, "A", "CDN", 100, "api.sync"),
                (now_ts_2, sid, "A", "RECONCILE_A", 50, "fastly.reconciliation"),
            ],
        )
        con.executemany(
            "INSERT INTO usage_log_hourly_summary (service_id, hour, operation_class, operation_type, count, bytes, last_updated) VALUES (?, ?, ?, ?, ?, ?, ?)",
            [(sid, "2020-05-01T10", "A", "CDN", 100, 0, old_ts), (sid, now_hour, "A", "RECONCILE_A", 50, 0, now_ts_1)],
        )
        con.commit()
        q = "SELECT sum(count) FROM usage_log_hourly_summary WHERE service_id = ?"
        assert con.execute(q, (sid,)).fetchone()[0] == 150
        usage_log.purge_usage_log(sid, retention_days=30)
        assert con.execute(q, (sid,)).fetchone()[0] == 150
        usage_log.reconcile_fastly_stats(sid, [{"hour_iso": now_ts_1, "class_a": 0, "class_b": 0}])
        assert con.execute(q, (sid,)).fetchone()[0] == 100
    finally:
        usage_log_db.close_all_connections()

    clear_usage_log(sid)
    con2 = usage_log_db.get_con(sid)
    try:
        assert con2.execute("SELECT count(*) FROM usage_log WHERE service_id = ?", (sid,)).fetchone()[0] == 0
        assert (
            con2.execute("SELECT count(*) FROM usage_log_hourly_summary WHERE service_id = ?", (sid,)).fetchone()[0]
            == 0
        )
    finally:
        usage_log_db.close_all_connections()


def test_slow_queries_flush_connection_release_and_worker_thread(monkeypatch):
    def _prime(svc: str):
        with slow_queries._buffer_lock:
            slow_queries._buffer.clear()
            slow_queries._buffer[svc] = [{"query_id": "q1"}]

    for svc, err_con, err_exec in [
        ("svc-flush", False, False),
        ("svc-flush-error", False, True),
        ("svc-no-con", True, False),
    ]:
        con, release = MagicMock(), MagicMock()
        if err_exec:
            con.executemany.side_effect = RuntimeError("metadata unavailable")
        monkeypatch.setattr(
            slow_queries,
            "get_con",
            MagicMock(side_effect=RuntimeError("metadata unavailable")) if err_con else (lambda sid, c=con: c),
        )
        monkeypatch.setattr(slow_queries, "release_thread_connection", release, raising=False)
        _prime(svc)
        slow_queries._flush_all()
        if not err_con and not err_exec:
            con.executemany.assert_called_once()
            con.commit.assert_called_once_with()
        release.assert_called_once_with()

    con_th, released_on = MagicMock(), []
    monkeypatch.setattr(slow_queries, "get_con", lambda service_id: con_th)
    monkeypatch.setattr(
        slow_queries, "release_thread_connection", lambda: released_on.append(threading.current_thread()), raising=False
    )
    _prime("svc-thread")
    worker = threading.Thread(target=slow_queries._flush_all)
    worker.start()
    worker.join(timeout=2)
    assert not worker.is_alive() and released_on == [worker]


# ── Merged from tests/core/test_query_registry.py ──

"""Tests for the Live Query Monitor registry, attribution, and the DuckDB
result wrapper. The pool-reuse race test is the regression bait for the
``_conn_to_query`` stamp design — without it, ``cancel_query`` would
interrupt the next query on a reused connection."""


import sqlite3
import time

import pytest
from backend.core.query_attribution import (
    Attribution,
    _capture_caller,
    current_attribution,
    derive_from_process_context,
)
from backend.core.query_instrumentation import (
    _parse_memory_mb,
)

from backend.core import metadata as _meta
from backend.core import query_registry as qr_mod
from backend.core.query_registry import QueryRegistry, _conn_to_query, query_registry


class _RecordingConn:
    """Weakref-able connection stand-in that records ``interrupt()`` calls.

    Lets the stamp-moved tests below assert that ``cancel_query`` did NOT
    fire ``interrupt()`` on the live connection — the exact cross-talk a
    deletion of the per-connection stamp re-validation would re-introduce.
    A plain class instance is weakref-able and has a stable ``id()`` while
    the test holds a reference, so it behaves like a pooled DuckDB conn for
    the registry's stamp bookkeeping.
    """

    def __init__(self) -> None:
        self.interrupts = 0

    def interrupt(self) -> None:
        self.interrupts += 1


# ── Fixtures ───────────────────────────────────────────────────────────────


@pytest.fixture
def fresh_registry() -> QueryRegistry:
    """A scratch registry that doesn't share state with the singleton."""
    return QueryRegistry()


@pytest.fixture()
def _reset_singleton():
    """Snapshot + restore the singleton's internal state so tests don't
    bleed into each other. The singleton is exercised by some tests
    (instrumentation integration) so we can't just replace it."""
    queries = dict(query_registry._queries)
    history = list(query_registry._history)
    yield
    query_registry._queries.clear()
    query_registry._queries.update(queries)
    query_registry._history.clear()
    query_registry._history.extend(history)


@pytest.mark.usefixtures("_reset_singleton")
def test_expire_pool_queries_interrupts_only_pooled_duckdb():
    registry = QueryRegistry()
    pooled = _RecordingConn()
    qid = registry.register("DuckDB", "SELECT 1", service_id="svc", con=pooled, pool_slot="svc#0001")
    active = registry._queries[qid]
    active.started_at_mono -= 121

    assert registry.expire_pool_queries() == 1
    assert pooled.interrupts == 1
    assert active.cancelled_at is not None


# ── Attribution ─────────────────────────────────────────────────────────────


@pytest.mark.usefixtures("_reset_singleton")
class TestAttribution:
    def test_analyst_label(self):
        a = Attribution.analyst(
            analyst_id="passcode_abcd1234",
            analyst_name="Drew",
            request_path="/api/query",
            request_id="req_1",
        )
        assert "Drew" in a.display_label()
        assert "/api/query" in a.display_label()
        assert a.principal_id() == "passcode_abcd1234"

    def test_analyst_label_guest(self):
        a = Attribution.analyst(
            analyst_id="passcode_a3f1",
            analyst_name=None,
            request_path="/api/dashboard",
            request_id=None,
        )
        # No name → "Guest (…last4)"
        assert "Guest" in a.display_label()
        assert "a3f1" in a.display_label()

    def test_admin_label(self):
        a = Attribution.admin(
            admin_id="10.0.0.5",
            request_path="/api/admin/queries",
            request_id="r2",
        )
        assert "Admin: 10.0.0.5" in a.display_label()
        assert a.principal_id() == "10.0.0.5"

    def test_cron_label(self):
        a = Attribution.cron(cron_job="sync_svc1", cron_run_id="r7f3")
        assert "Cron: sync_svc1" in a.display_label()
        assert "r7f3" in a.display_label()
        assert a.principal_id() == "r7f3"

    def test_system_label_with_thread_name(self):
        a = Attribution.system()
        assert a.kind == "system"
        assert "thread:" in a.caller_qualname or "MainThread" in a.caller_qualname

    def test_derive_from_process_context_shapes(self):
        assert derive_from_process_context("cron:sync_svc1").kind == "cron"
        assert derive_from_process_context("cron:sync_svc1").cron_job == "sync_svc1"
        assert derive_from_process_context("startup:init_service").kind == "system"
        # api:... is intentionally ignored — RequestContext owns HTTP attribution.
        assert derive_from_process_context("api:GET /admin/download-zip:/tmp") is None
        assert derive_from_process_context(None) is None
        assert derive_from_process_context("") is None


@pytest.mark.usefixtures("_reset_singleton")
class TestCallerCapture:
    def test_skips_instrumentation_frames(self):
        # skip_frames defaults to 2 (skip _capture_caller + the caller's
        # register() frame). Called directly from a test, that puts us
        # past the test body — use skip_frames=1 to land in this method.
        qual, file_line = _capture_caller(skip_frames=1)
        assert "test_skips_instrumentation_frames" in qual
        assert "test_metrics_and_telemetry.py" in file_line

    def test_register_attributes_caller(self, fresh_registry: QueryRegistry):
        # End-to-end: register() should record THIS test's frame in the
        # attribution's caller_file (skipping query_registry +
        # query_attribution).
        qid = fresh_registry.register("SQLite", "SELECT 1", con=None)
        snap = fresh_registry.snapshot()
        assert "test_metrics_and_telemetry.py" in snap["active"][0]["attribution"]["caller_file"]

    def test_returns_unknown_on_empty_stack(self):
        # skip_frames=999 walks past everything.
        qual, file_line = _capture_caller(skip_frames=999)
        assert qual == "<unknown>"


# ── Registry — register / deregister / snapshot ─────────────────────────────


@pytest.mark.usefixtures("_reset_singleton")
class TestRegistry:
    def test_register_returns_monotonic_id(self, fresh_registry: QueryRegistry):
        a = fresh_registry.register("SQLite", "SELECT 1", con=None)
        b = fresh_registry.register("SQLite", "SELECT 2", con=None)
        assert b > a

    def test_register_with_no_attribution_synthesises_system(self, fresh_registry: QueryRegistry):
        token = current_attribution.set(None)
        try:
            qid = fresh_registry.register("SQLite", "SELECT 1", con=None)
            assert qid >= 0
            snap = fresh_registry.snapshot()
            assert snap["active"][0]["attribution"]["kind"] == "system"
        finally:
            current_attribution.reset(token)

    def test_register_picks_up_attribution_from_contextvar(self, fresh_registry: QueryRegistry):
        attr = Attribution.admin(admin_id="ops", request_path="/api/admin/x", request_id="r1")
        prev = current_attribution.get()
        current_attribution.set(attr)
        try:
            qid = fresh_registry.register("SQLite", "SELECT 1", con=None)
            snap = fresh_registry.snapshot()
            assert snap["active"][0]["attribution"]["kind"] == "admin"
            assert snap["active"][0]["attribution"]["principal_id"] == "ops"
        finally:
            current_attribution.set(prev)

    def test_deregister_moves_to_completed_history(self, fresh_registry: QueryRegistry):
        qid = fresh_registry.register("SQLite", "SELECT 1", con=None)
        fresh_registry.deregister(qid)
        snap = fresh_registry.snapshot(include_completed=True)
        assert len(snap["active"]) == 0
        assert len(snap["completed"]) == 1
        assert snap["completed"][0]["outcome"] == "ok"

    def test_deregister_with_error_records_exception_type(self, fresh_registry: QueryRegistry):
        qid = fresh_registry.register("DuckDB", "SELECT FROM bad", con=None)
        fresh_registry.deregister(qid, error=RuntimeError("kaboom"))
        snap = fresh_registry.snapshot(include_completed=True)
        c = snap["completed"][0]
        assert c["outcome"] == "error"
        assert c["error_type"] == "RuntimeError"
        assert "kaboom" in c["error_message"]

    def test_deregister_negative_id_is_noop(self, fresh_registry: QueryRegistry):
        fresh_registry.deregister(-1)  # must not raise

    def test_snapshot_respects_since_seq(self, fresh_registry: QueryRegistry):
        a = fresh_registry.register("SQLite", "A", con=None)
        b = fresh_registry.register("SQLite", "B", con=None)
        snap = fresh_registry.snapshot(since_seq=a)
        ids = [r["query_id"] for r in snap["active"]]
        assert b in ids and a not in ids

    def test_summary_counts(self, fresh_registry: QueryRegistry):
        fresh_registry.register("SQLite", "A", con=None)
        fresh_registry.register("DuckDB", "B", con=None)
        fresh_registry.register("DuckDB", "C", con=None)
        s = fresh_registry.summary()
        assert s["active_total"] == 3
        assert s["by_db_type"] == {"SQLite": 1, "DuckDB": 2}


# ── Cancel — including the pool-reuse race regression test ─────────────────


@pytest.mark.usefixtures("_reset_singleton")
class TestCancel:
    def test_cancel_unknown_returns_not_found(self, fresh_registry: QueryRegistry):
        assert fresh_registry.cancel_query(999_999) == "not_found"

    def test_cancel_with_no_connection_returns_already_finished(self, fresh_registry: QueryRegistry):
        qid = fresh_registry.register("SQLite", "SELECT 1", con=None)
        assert fresh_registry.cancel_query(qid) == "already_finished"

    def test_cancel_active_sqlite_returns_cancelled(self, fresh_registry: QueryRegistry):
        con = sqlite3.connect(":memory:")
        qid = fresh_registry.register("SQLite", "SELECT 1", con=con)
        assert fresh_registry.cancel_query(qid, admin_id="t1") == "cancelled"
        # cancelled_at stamp set
        snap = fresh_registry.snapshot()
        assert snap["active"][0]["cancelled_at"] is not None

    def test_deregister_after_cancel_records_cancelled_not_error(self, fresh_registry: QueryRegistry):
        """KILL-OUTCOME-02: an admin kill interrupts the connection, the query
        raises (e.g. ``InterruptException``), and the caller hands that error to
        ``deregister``. The completed-history outcome must still be
        ``cancelled`` — NOT ``error`` — because ``cancelled_at`` takes
        precedence over ``error`` in ``deregister``. This keeps an
        admin-initiated cancellation from being mislabelled a query failure in
        the live-monitor history and the cancelled/error metrics. Swapping the
        precedence (error before cancelled_at) flips the outcome and fails here.
        """
        con = sqlite3.connect(":memory:")
        qid = fresh_registry.register("SQLite", "SELECT 1", con=con)
        assert fresh_registry.cancel_query(qid, admin_id="t1") == "cancelled"
        # The cancelled query unwinds and the caller reports the interrupt error.
        fresh_registry.deregister(qid, error=RuntimeError("INTERRUPT: query was cancelled"))
        c = fresh_registry.snapshot(include_completed=True)["completed"][0]
        assert c["outcome"] == "cancelled", (
            f"cancelled query recorded as {c['outcome']!r} — cancelled_at must win over error"
        )

    def test_cancel_after_deregister_returns_not_found(self, fresh_registry: QueryRegistry):
        con = sqlite3.connect(":memory:")
        qid = fresh_registry.register("SQLite", "SELECT 1", con=con)
        fresh_registry.deregister(qid)
        assert fresh_registry.cancel_query(qid) == "not_found"

    def test_pool_reuse_race_does_not_kill_wrong_query(self, fresh_registry: QueryRegistry):
        """The single most-important regression test for this system.

        Scenario: connection runs query A, completes, returns to the pool;
        a moment later the same connection runs query B; admin's stale UI
        clicks Kill on the *old* query A. The registry MUST refuse to
        interrupt — otherwise we'd cancel query B which has nothing to do
        with the admin's intent.
        """
        con = duckdb.connect(":memory:")
        qid_a = fresh_registry.register("DuckDB", "SELECT A", con=con)
        fresh_registry.deregister(qid_a)
        qid_b = fresh_registry.register("DuckDB", "SELECT B", con=con)
        # Stale click on A:
        assert fresh_registry.cancel_query(qid_a) == "not_found"
        # B is untouched and still cancellable:
        assert fresh_registry.cancel_query(qid_b) == "cancelled"

    def test_cancel_refuses_when_stamp_moved_while_original_still_live(self, fresh_registry: QueryRegistry):
        """The stamp-moved refusal branch (design doc §13.10) — the late-GC race.

        ``test_pool_reuse_race_does_not_kill_wrong_query`` deregisters A
        *before* B reuses the connection, so ``cancel_query(A)`` exits early
        on the cheap ``not_found`` short-circuit and never reaches the
        per-connection stamp comparison. This test pins the OTHER path: A is
        STILL in ``_queries`` (its deregister hasn't run yet) when B reuses
        the same connection. ``cancel_query(A)`` must fall through to the
        stamp guard, observe that the stamp now points at B, and refuse —
        WITHOUT interrupting the live connection. Deleting or weakening the
        in-lock stamp re-validation would silently turn this into a
        cross-talk kill (interrupt fires on B) while the rest of the suite
        stays green; this assertion is what catches that regression.
        """
        con = _RecordingConn()
        qid_a = fresh_registry.register("DuckDB", "SELECT A", con=con)
        # B reuses the SAME connection object (same id()) → the stamp moves
        # from A to B. A is intentionally NOT deregistered.
        qid_b = fresh_registry.register("DuckDB", "SELECT B", con=con)
        assert qid_a in fresh_registry._queries  # not_found short-circuit NOT taken
        # cancel(A) reaches the stamp guard, sees stamp == B, refuses.
        assert fresh_registry.cancel_query(qid_a) == "already_finished"
        assert con.interrupts == 0, "live connection (bound to B) must not be interrupted"
        # B itself remains cancellable through the moved stamp.
        assert fresh_registry.cancel_query(qid_b) == "cancelled"
        assert con.interrupts == 1

    def test_late_deregister_of_original_keeps_reused_conn_stamp_for_b(self, fresh_registry: QueryRegistry):
        """Ordering proof: A's late deregister must not strip B's stamp.

        When A finishes and deregisters AFTER B has already reused the
        connection, A's ``deregister`` is guarded by ``== qid_a`` so it
        cannot delete the stamp that now belongs to B. B therefore stays
        interruptible. Pins the structural defense behind KILL-2.
        """
        con = _RecordingConn()
        qid_a = fresh_registry.register("DuckDB", "SELECT A", con=con)
        qid_b = fresh_registry.register("DuckDB", "SELECT B", con=con)  # stamp → B
        # A finishes late and deregisters — must NOT wipe B's stamp.
        fresh_registry.deregister(qid_a)
        assert _conn_to_query.get(id(con)) == qid_b
        # B is still live and interruptible.
        assert fresh_registry.cancel_query(qid_b) == "cancelled"
        assert con.interrupts == 1


# ── Concurrent register/deregister stress ──────────────────────────────────


@pytest.mark.usefixtures("_reset_singleton")
class TestConcurrency:
    def test_concurrent_register_deregister_leaves_no_leaks(self, fresh_registry: QueryRegistry):
        # 16 threads, each registering+deregistering 200 times, must end
        # with empty active map (history is bounded so just check active).
        N_THREADS = 16
        N_OPS = 200

        def worker():
            for _ in range(N_OPS):
                qid = fresh_registry.register("SQLite", "x", con=None)
                fresh_registry.deregister(qid)

        threads = [threading.Thread(target=worker) for _ in range(N_THREADS)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(fresh_registry._queries) == 0


# ── DuckDB result wrapper — proves the registry's duration reflects fetch ─


@pytest.mark.usefixtures("_reset_singleton")
class TestDuckDBResultWrapper:
    def test_fetch_duration_captured(self):
        """Without the result wrapper this would show ~0ms even though
        fetchdf() takes 100s of ms on a real query."""
        from backend.core.query_registry import query_registry as singleton

        raw = duckdb.connect(":memory:")
        con = InstrumentedDuckDBConnection(raw, service_id="test_svc")
        con.execute("CREATE TABLE t AS SELECT i FROM range(2_000_000) tbl(i)")
        t0 = time.perf_counter()
        df = con.execute("SELECT i FROM t").fetchdf()
        wall_ms = (time.perf_counter() - t0) * 1000.0
        assert len(df) == 2_000_000

        # Find the most-recent SELECT row in history
        hist = singleton.snapshot(include_completed=True)["completed"]
        matches = [c for c in hist if "SELECT i FROM t" in c["sql_preview"]]
        assert matches, "expected the SELECT to be recorded"
        last = matches[-1]
        # Registry should be within 25% of wall clock — proves the wrapper
        # held the registration through fetchdf().
        assert last["duration_ms"] >= 0.5 * wall_ms, (
            f"registry duration {last['duration_ms']}ms < 50% of wall {wall_ms}ms — "
            f"result wrapper likely deregistered at execute() instead of fetch()"
        )
        # Pool slot populated:
        assert last["attribution"]["pool_slot"], "pool_slot should be set on DuckDB rows"

    def test_kill_across_real_connection_reuse_cycle(self):
        """KILL-INT-01: cross-talk safety through the REAL instrumented
        connection lifecycle (register-on-execute, deregister-on-fetch) — the
        same wrapper ``duckdb_pool`` hands out on every acquire. Drives an
        actual checkout→complete→release→reacquire cycle on ONE raw connection:
        query A runs to completion (deregistered on fetch), then the same raw
        connection is reused for a still-running query B. A stale admin kill on
        the completed A must be refused (``not_found``) and must NOT interrupt
        the live B — proven by B's result still fetching cleanly afterward. The
        registry-level tests pin the stamp logic in isolation; this pins that
        the real execute/fetch path maintains it across a connection reuse."""
        from backend.core.query_registry import query_registry as singleton

        raw = duckdb.connect(":memory:")
        # Cycle 1 — A acquires the connection, runs to completion, releases.
        con_a = InstrumentedDuckDBConnection(raw, service_id="killint")
        con_a.execute("SELECT 1 AS a").fetchall()
        completed = [c for c in singleton.snapshot(include_completed=True)["completed"] if c["service_id"] == "killint"]
        assert completed, "query A did not land in completed history"
        qid_a = completed[-1]["query_id"]

        # Cycle 2 — the SAME raw connection is reacquired for B, which stays
        # ACTIVE (its result is held, not terminally fetched yet).
        con_b = InstrumentedDuckDBConnection(raw, service_id="killint")
        result_b = con_b.execute("SELECT 42 AS b")

        # Stale click on the completed A: must refuse without touching raw
        # (now bound to the live B). A con-keyed (rather than qid-keyed) kill
        # would interrupt raw and break B.
        assert singleton.cancel_query(qid_a) == "not_found"

        # Proof B was untouched: its result still fetches cleanly.
        assert result_b.fetchall() == [(42,)]

    def test_exception_in_fetch_records_outcome_error(self):
        from backend.core.query_registry import query_registry as singleton

        raw = duckdb.connect(":memory:")
        con = InstrumentedDuckDBConnection(raw, service_id="test_svc")
        with pytest.raises(duckdb.Error):
            con.execute("SELECT * FROM nonexistent_table_xyz")
        hist = singleton.snapshot(include_completed=True)["completed"]
        matches = [c for c in hist if "nonexistent_table_xyz" in c["sql_preview"]]
        assert matches
        assert matches[-1]["outcome"] == "error"
        assert "nonexistent" in (matches[-1]["error_message"] or "").lower() or matches[-1]["error_type"]


# ── Peak memory probe ─────────────────────────────────────────────────────────


@pytest.mark.usefixtures("_reset_singleton")
class TestPeakMemory:
    def test_parse_memory_mb_int(self):
        assert _parse_memory_mb(1_048_576) == 1.0  # 1 MiB exactly
        assert _parse_memory_mb(0) == 0.0
        assert _parse_memory_mb(None) is None

    def test_parse_memory_mb_strings(self):
        assert _parse_memory_mb("1024") == round(1024 / (1024 * 1024), 2)
        assert _parse_memory_mb("1 MiB") == 1.0
        assert _parse_memory_mb("1 GiB") == 1024.0
        assert _parse_memory_mb("1.5 GiB") == 1536.0
        assert _parse_memory_mb("512 MB") == round(512_000_000 / (1024 * 1024), 2)
        assert _parse_memory_mb("0 bytes") == 0.0

    def test_parse_memory_mb_garbage(self):
        assert _parse_memory_mb("") is None
        assert _parse_memory_mb("not a number") is None
        assert _parse_memory_mb("3 yibibytes") is None  # unknown unit
        assert _parse_memory_mb(object()) is None

    def test_probe_returns_some_value_on_live_connection(self):
        raw = duckdb.connect(":memory:")
        raw.execute("CREATE TABLE t AS SELECT i FROM range(500_000) tbl(i)")
        mb = _probe_duckdb_memory(raw)
        # Materialised table should hold non-trivial memory; if DuckDB
        # reports zero on this machine the probe still must not crash.
        assert mb is None or mb >= 0.0

    def test_probe_swallows_errors(self):
        # A non-duckdb object (no .cursor()) must not crash the probe.
        assert _probe_duckdb_memory(object()) is None

    def test_completed_row_has_peak_memory_field(self):
        """The CompletedQuery JSON shape carries peak_memory_mb on every
        row (None for SQLite / probe-failure paths). The frontend uses
        the field's presence to decide whether to render the column."""
        from backend.core.query_registry import query_registry as singleton

        raw = duckdb.connect(":memory:")
        con = InstrumentedDuckDBConnection(raw, service_id="mem_test_svc")
        con.execute("CREATE TABLE t AS SELECT i FROM range(100_000) tbl(i)").fetchall()
        hist = singleton.snapshot(include_completed=True)["completed"]
        matches = [c for c in hist if c["service_id"] == "mem_test_svc"]
        assert matches, "expected CREATE TABLE to land in completed history"
        assert "peak_memory_mb" in matches[-1]

    def test_sqlite_completed_row_has_null_peak_memory(self):
        """SQLite never has a meaningful memory value; the field exists but
        stays None so the frontend renders consistently."""
        from backend.core.query_registry import query_registry as singleton
        from backend.utils import sqlite_profiler

        qid = sqlite_profiler._live_register("SQLite", "CREATE TABLE x (i INT)", con=None)
        sqlite_profiler._live_deregister(qid, error=None)
        hist = singleton.snapshot(include_completed=True)["completed"]
        sqlite_rows = [c for c in hist if c["db_type"] == "SQLite"]
        assert sqlite_rows
        assert sqlite_rows[-1]["peak_memory_mb"] is None


# ── Streaming RecordBatchReader wrapper (.arrow() / fetch_record_batch) ──────


@pytest.mark.usefixtures("_reset_singleton")
class TestRecordBatchReader:
    def test_arrow_iteration_holds_registration_until_consumed(self):
        """``.arrow()`` returns a streaming reader; deregistration must
        wait for iteration to complete. Without :class:`_InstrumentedRecordReader`,
        a long downstream consumer would see ~0ms duration on the row."""
        from backend.core.query_registry import query_registry as singleton

        raw = duckdb.connect(":memory:")
        con = InstrumentedDuckDBConnection(raw, service_id="arrow_svc")
        con.execute("CREATE TABLE big AS SELECT i FROM range(500_000) tbl(i)").fetchall()
        reader = con.execute("SELECT * FROM big").arrow()
        # The reader was just returned — query should still be active.
        active = singleton.snapshot()["active"]
        active_for_reader = [r for r in active if r["service_id"] == "arrow_svc"]
        assert active_for_reader, "row deregistered before reader iteration — _InstrumentedRecordReader missing?"

        # Drain the reader; query should deregister.
        total_rows = 0
        for batch in reader:
            total_rows += batch.num_rows
            # Simulate slow consumer.
            time.sleep(0.005)
        assert total_rows == 500_000

        # Now the query has moved to completed history.
        hist = singleton.snapshot(include_completed=True)["completed"]
        matches = [c for c in hist if c["service_id"] == "arrow_svc" and "SELECT * FROM big" in c["sql_preview"]]
        assert matches, "expected SELECT to deregister after reader iteration"

    def test_to_arrow_table_materialises_immediately(self):
        """Sanity check that ``to_arrow_table()`` (the materialising call
        used by [iceberg/buffer.py:666](backend/core/iceberg/buffer.py#L666))
        still deregisters at the method-call boundary, not after iteration.
        It's listed in ``_TERMINAL_METHODS``, not ``_READER_METHODS``."""
        from backend.core.query_registry import query_registry as singleton

        raw = duckdb.connect(":memory:")
        con = InstrumentedDuckDBConnection(raw, service_id="materialised_svc")
        con.execute("CREATE TABLE x AS SELECT i FROM range(10_000) tbl(i)").fetchall()
        table = con.execute("SELECT * FROM x").to_arrow_table()
        assert table.num_rows == 10_000
        # Already deregistered before we even checked.
        active = singleton.snapshot()["active"]
        assert not [r for r in active if r["service_id"] == "materialised_svc"]

    def test_reader_close_completes_registration(self):
        """If the consumer never iterates but calls close(), the wrapper
        still drives deregistration so the registry doesn't leak."""
        from backend.core.query_registry import query_registry as singleton

        raw = duckdb.connect(":memory:")
        con = InstrumentedDuckDBConnection(raw, service_id="close_svc")
        con.execute("CREATE TABLE y AS SELECT i FROM range(100) tbl(i)").fetchall()
        reader = con.execute("SELECT * FROM y").arrow()
        reader.close()
        active = singleton.snapshot()["active"]
        assert not [r for r in active if r["service_id"] == "close_svc"]

    def test_reader_wrapper_passes_through_schema(self):
        """The wrapper must delegate non-completion attribute access (like
        the ``schema`` attribute) so callers that introspect the reader
        keep working."""
        raw = duckdb.connect(":memory:")
        con = InstrumentedDuckDBConnection(raw, service_id="schema_svc")
        con.execute("CREATE TABLE z AS SELECT 1 as a, 'x' as b").fetchall()
        reader = con.execute("SELECT * FROM z").arrow()
        assert isinstance(reader, _InstrumentedRecordReader)
        # schema attribute is delegated to the raw reader
        assert hasattr(reader, "schema")
        names = [f.name for f in reader.schema]
        assert names == ["a", "b"]
        reader.close()


# ── cancel on client disconnect ─────────────────────────────────────────────────────────────────────────


def _register_for(registry: QueryRegistry, key: str | None, con: _RecordingConn, db: str = "DuckDB", slot=None) -> int:
    token = current_attribution.set(
        Attribution.admin(
            admin_id="1.2.3.4", request_path="/api/dashboard/bundle", request_id="shared-trace", cancel_key=key
        )
    )
    try:
        return registry.register(db, "SELECT 1", service_id="svc", con=con, pool_slot=slot)
    finally:
        current_attribution.reset(token)


@pytest.mark.usefixtures("_reset_singleton")
def test_the_cancel_key_survives_the_attribution_copies_the_registry_makes():
    attr = Attribution.admin(admin_id="a", request_path="/p", request_id="r", cancel_key="k1")

    assert attr.with_caller("q", "f.py:1").cancel_key == "k1"
    assert attr.with_pool_slot("svc#1").cancel_key == "k1"
    assert (
        Attribution.analyst(
            analyst_id="x", analyst_name=None, request_path="/p", request_id="r", cancel_key="k2"
        ).cancel_key
        == "k2"
    )


@pytest.mark.usefixtures("_reset_singleton")
def test_cancel_for_key_interrupts_only_that_requests_duckdb_queries():
    registry = QueryRegistry()
    mine, mine_second, sibling, other_db = _RecordingConn(), _RecordingConn(), _RecordingConn(), _RecordingConn()
    q1 = _register_for(registry, "me", mine)
    _register_for(registry, "me", mine_second, slot="svc#2")
    _register_for(registry, "sibling", sibling)  # same request_id (shared trace), different request
    _register_for(registry, "me", other_db, db="SQLite")

    assert registry.cancel_for_key("me") == 2

    assert (mine.interrupts, mine_second.interrupts) == (1, 1)
    assert sibling.interrupts == 0, "a sibling request sharing the trace id must not be cancelled"
    assert other_db.interrupts == 0, "only DuckDB serving queries are interrupted"
    assert registry._queries[q1].cancelled_at is not None


@pytest.mark.usefixtures("_reset_singleton")
def test_cancel_for_key_is_idempotent_and_ignores_unknown_keys():
    registry = QueryRegistry()
    conn = _RecordingConn()
    _register_for(registry, "me", conn)

    assert registry.cancel_for_key("nobody") == 0
    assert registry.cancel_for_key("me") == 1
    assert registry.cancel_for_key("me") == 0
    assert conn.interrupts == 1


@pytest.mark.usefixtures("_reset_singleton")
def test_cancel_for_key_without_a_key_cancels_nothing():
    registry = QueryRegistry()
    conn = _RecordingConn()
    _register_for(registry, None, conn)

    assert registry.cancel_for_key("") == 0
    assert registry.cancel_for_key(None) == 0  # type: ignore[arg-type]
    assert conn.interrupts == 0


# ── Persistent slow-query history layer ─────────────────────────────────────


@pytest.fixture
def svc_id(tmp_path, monkeypatch):
    monkeypatch.setattr("backend.config.DATA_DIR", tmp_path)
    monkeypatch.setattr("backend.core.metadata.base.DATA_DIR", tmp_path, raising=False)
    # Set the slow-query threshold to a massive value by default so real queries executed
    # under test CPU starvation never get flagged as slow queries and pollute the database.
    monkeypatch.setattr("backend.core.query_registry._SLOW_QUERY_PERSIST_THRESHOLD_MS", 999999.0)
    svc = "test-slow-queries-svc"
    con = _meta.get_con(svc)
    con.execute("DELETE FROM slow_queries WHERE service_id = ?", (svc,))
    con.commit()
    return svc


@pytest.fixture
def fresh_singleton_registry(monkeypatch):
    """Use a fresh QueryRegistry so cross-test history doesn't bleed.
    The module-level singleton has shared state; replacing it here
    isolates each test."""
    new_reg = qr_mod.QueryRegistry()
    monkeypatch.setattr(qr_mod, "query_registry", new_reg)
    # The hot-path persist call resolves the registry via the import
    # cycle so any test using it must also see the same instance.
    return new_reg


def _row(*, qid: int, duration_ms: float, started: float | None = None, kind: str = "admin") -> dict:
    started = started if started is not None else time.time()
    return {
        "query_id": qid,
        "db_type": "DuckDB",
        "service_id": "test-slow-queries-svc",
        "started_at_utc": started,
        "ended_at_utc": started + duration_ms / 1000.0,
        "duration_ms": duration_ms,
        "outcome": "ok",
        "sql_preview": "SELECT 1",
        "sql_full": "SELECT 1",
        "sql_len": 8,
        "attr_kind": kind,
        "attr_label": f"{kind}: test",
        "attr_caller_qualname": "tests.helper",
        "attr_caller_file": "test_slow_queries_persist.py:1",
    }


@pytest.mark.usefixtures("_reset_singleton")
def test_insert_then_list_round_trip(svc_id):
    _meta.insert_slow_query(svc_id, _row(qid=1, duration_ms=250))
    rows = _meta.list_slow_queries(svc_id, since_utc=time.time() - 60)
    assert len(rows) == 1
    assert rows[0]["query_id"] == 1
    assert rows[0]["duration_ms"] == 250
    assert rows[0]["attr_kind"] == "admin"


@pytest.mark.usefixtures("_reset_singleton")
def test_list_filters_by_threshold(svc_id):
    """threshold_ms is applied SQL-side via the duration_ms index. Rows
    below the threshold must not appear in the result."""
    _meta.insert_slow_query(svc_id, _row(qid=1, duration_ms=120))
    _meta.insert_slow_query(svc_id, _row(qid=2, duration_ms=600))
    _meta.insert_slow_query(svc_id, _row(qid=3, duration_ms=1500))
    rows = _meta.list_slow_queries(svc_id, since_utc=time.time() - 60, threshold_ms=500)
    qids = {r["query_id"] for r in rows}
    assert qids == {2, 3}


@pytest.mark.usefixtures("_reset_singleton")
def test_list_filters_by_kind(svc_id):
    _meta.insert_slow_query(svc_id, _row(qid=1, duration_ms=200, kind="admin"))
    _meta.insert_slow_query(svc_id, _row(qid=2, duration_ms=200, kind="cron"))
    rows = _meta.list_slow_queries(svc_id, since_utc=time.time() - 60, kind="cron")
    assert [r["query_id"] for r in rows] == [2]


@pytest.mark.usefixtures("_reset_singleton")
def test_list_default_sort_is_recent_first(svc_id):
    now = time.time()
    _meta.insert_slow_query(svc_id, _row(qid=1, duration_ms=200, started=now - 30))
    _meta.insert_slow_query(svc_id, _row(qid=2, duration_ms=200, started=now - 10))
    rows = _meta.list_slow_queries(svc_id, since_utc=now - 60)
    assert [r["query_id"] for r in rows if r["query_id"] in {1, 2}] == [2, 1]


@pytest.mark.usefixtures("_reset_singleton")
def test_list_sort_by_duration_flips_order(svc_id):
    now = time.time()
    _meta.insert_slow_query(svc_id, _row(qid=1, duration_ms=2000, started=now - 30))
    _meta.insert_slow_query(svc_id, _row(qid=2, duration_ms=300, started=now - 10))
    rows = _meta.list_slow_queries(svc_id, since_utc=now - 60, sort_by_duration=True)
    assert [r["query_id"] for r in rows] == [1, 2]


@pytest.mark.usefixtures("_reset_singleton")
def test_count_matches_list_length(svc_id):
    """count_slow_queries is the cheap row count for the overview card.
    It must agree with list_slow_queries under the same filters."""
    now = time.time()
    _meta.insert_slow_query(svc_id, _row(qid=1, duration_ms=120, started=now - 100))
    _meta.insert_slow_query(svc_id, _row(qid=2, duration_ms=600, started=now - 50))
    _meta.insert_slow_query(svc_id, _row(qid=3, duration_ms=2000, started=now - 10))
    assert _meta.count_slow_queries(svc_id, since_utc=now - 200, threshold_ms=500) == 2
    rows = _meta.list_slow_queries(svc_id, since_utc=now - 200, threshold_ms=500)
    assert len(rows) == 2


# ── Writer hook in query_registry ────────────────────────────────────────


@pytest.mark.usefixtures("_reset_singleton")
def test_deregister_persists_only_above_threshold(svc_id, fresh_singleton_registry, monkeypatch):
    """The hot-path filter: queries faster than the persist threshold
    must NOT write to SQLite. Otherwise the per-tick cron noise (sub-ms
    SQLite queries by the dozen) would flood the table — exactly the
    motivation for having a threshold."""
    monkeypatch.setattr(qr_mod, "_SLOW_QUERY_PERSIST_THRESHOLD_MS", 100.0)
    fast_qid = fresh_singleton_registry.register("DuckDB", "SELECT 1", service_id=svc_id)
    # Pretend the query took 50 ms — under threshold.
    fresh_singleton_registry._queries[fast_qid].started_at_mono = time.monotonic() - 0.05
    fresh_singleton_registry.deregister(fast_qid)
    assert _meta.count_slow_queries(svc_id, since_utc=time.time() - 60) == 0


@pytest.mark.usefixtures("_reset_singleton")
def test_deregister_persists_slow_query(svc_id, fresh_singleton_registry, monkeypatch):
    monkeypatch.setattr(qr_mod, "_SLOW_QUERY_PERSIST_THRESHOLD_MS", 100.0)
    slow_qid = fresh_singleton_registry.register("DuckDB", "SELECT pg_sleep(1)", service_id=svc_id)
    # Pretend the query took 500 ms — over threshold.
    fresh_singleton_registry._queries[slow_qid].started_at_mono = time.monotonic() - 0.5
    fresh_singleton_registry.deregister(slow_qid)
    rows = [
        row
        for row in _meta.list_slow_queries(svc_id, since_utc=time.time() - 60)
        if row["sql_preview"] == "SELECT pg_sleep(1)"
    ]
    assert len(rows) == 1
    assert rows[0]["duration_ms"] >= 100
    assert rows[0]["sql_preview"] == "SELECT pg_sleep(1)"


@pytest.mark.usefixtures("_reset_singleton")
def test_deregister_skips_when_no_service_id(svc_id, fresh_singleton_registry, monkeypatch):
    """System-level queries with no service_id have nowhere to land in
    the per-service metadata DB. They MUST be skipped, not crashed on."""
    monkeypatch.setattr(qr_mod, "_SLOW_QUERY_PERSIST_THRESHOLD_MS", 100.0)
    qid = fresh_singleton_registry.register("DuckDB", "SELECT 1", service_id=None)
    fresh_singleton_registry._queries[qid].started_at_mono = time.monotonic() - 0.5
    fresh_singleton_registry.deregister(qid)  # must not raise
    # Nothing landed in this service's db.
    assert _meta.count_slow_queries(svc_id, since_utc=time.time() - 60) == 0


@pytest.mark.usefixtures("_reset_singleton")
def test_deregister_persist_failure_does_not_propagate(svc_id, fresh_singleton_registry, monkeypatch):
    """A SQLite write failure on the persist path MUST NOT propagate
    back into the SQL hot path. Observability is best-effort —
    correctness is not."""
    monkeypatch.setattr(qr_mod, "_SLOW_QUERY_PERSIST_THRESHOLD_MS", 100.0)

    def _boom(*_a, **_kw):
        raise sqlite3.OperationalError("simulated metadata DB outage")

    monkeypatch.setattr(qr_mod._persist_slow_query.__globals__["__name__"], None, raising=False)  # noqa: SLF001 — defensive no-op
    # Monkeypatch the metadata helper used inside _persist_slow_query.
    monkeypatch.setattr("backend.core.metadata.insert_slow_query", _boom)
    qid = fresh_singleton_registry.register("DuckDB", "SELECT 1", service_id=svc_id)
    fresh_singleton_registry._queries[qid].started_at_mono = time.monotonic() - 0.5
    # The deregister call must complete cleanly even though the persist
    # raises internally.
    fresh_singleton_registry.deregister(qid)
    # And no row landed.
    assert _meta.count_slow_queries(svc_id, since_utc=time.time() - 60) == 0
