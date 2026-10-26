"""ClickHouse HTTP boundary: no server, schema, or serving-path changes."""

from __future__ import annotations

import asyncio
import json
import os
import threading
import traceback
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, replace
from datetime import UTC, datetime, timedelta
from email.parser import BytesParser
from email.policy import default
from hashlib import sha256
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import MagicMock, patch
from uuid import UUID

import duckdb
import httpx
import pytest
from backend.core.clickhouse_manifest import Artifact, Dataset, PgManifest
from backend.core.clickhouse_publication import (
    PAYLOAD_COLUMNS,
    DurableBatch,
    FosArtifacts,
    build_batch_id,
    canonical_bytes,
    canonical_row,
    digest_rows,
    full_rebuild,
    publish_batch,
    replay_unpublished,
    verify_generation,
)
from backend.core.clickhouse_schema import CLICKHOUSE_SCHEMA_VERSION
from backend.core.metadata.pg_schema import pg_schema_statements
from tests._helpers.sources import TinyFos

from backend import config
from backend.core import clickhouse_client as ch
from backend.core import clickhouse_observer as clickhouse_metrics
from backend.core import clickhouse_observer as metrics
from backend.core import clickhouse_observer as observer
from backend.core import clickhouse_schema as schema
from backend.core.clickhouse_client import (
    CLICKHOUSE_FACT_COLUMNS,
    ClickHouseClient,
    ClickHouseError,
)
from backend.models.common import DebugQuery
from backend.utils import telemetry
from scripts import clickhouse_replay
from scripts.clickhouse_replay import parser, run


def row(ordinal=0):
    return ("raw/fixture.gz", ordinal, "2026-09-01 00:00:00.000000", "US", "192.0.2.1", "/", 1)


def fixture_manifest(service="TestPrototype", rows=None):
    rows = rows or (row(), row(1))
    dataset = Dataset(
        service,
        "dataset",
        "catalog",
        "logs_fixture",
        4,
        datetime(2026, 9, 1, tzinfo=UTC),
        datetime(2026, 9, 2, tzinfo=UTC),
        datetime.now(UTC) + timedelta(hours=1),
        len(rows),
        digest_rows(rows),
    )
    content_hash = sha256(b"immutable parquet").hexdigest()
    artifact = Artifact(
        service,
        dataset.dataset_id,
        build_batch_id(service, dataset.dataset_id, content_hash),
        "s3://test-bucket/clickhouse-prototype/fixture.parquet",
        content_hash,
        digest_rows(rows),
        100,
        0,
        len(rows),
    )
    return dataset, artifact, DurableBatch(artifact, rows)


def test_canonical_and_deterministic_identity_and_sqlite_rejection(monkeypatch):
    assert digest_rows((row(),)) != digest_rows((row(), row(1)))
    assert canonical_bytes(row()) != canonical_bytes((*row()[:3], None, *row()[4:]))
    cols = ("source_identity", "row_ordinal", "timestamp", "country", "ip", "url", "conn_requests")
    assert canonical_row(dict(zip(cols, row(), strict=True))) == row()
    assert build_batch_id("a", "d", "c") == build_batch_id("a", "d", "c")
    assert build_batch_id("a", "d", "c") != build_batch_id("b", "d", "c")
    assert build_batch_id("a", "d", "c") != build_batch_id("a", "e", "c")
    monkeypatch.delenv("METADATA_DSN", raising=False)
    with pytest.raises(RuntimeError, match="Postgres"):
        PgManifest()


@pytest.mark.parametrize("bad_rows", [(row(),), (row(), row(3))])
def test_bad_or_conflicting_payload_never_claims_or_inserts(bad_rows):
    dataset, artifact, batch = fixture_manifest()
    store, client = MagicMock(), MagicMock()
    store.artifact.return_value = artifact
    with pytest.raises(ValueError, match="payload"):
        publish_batch(dataset.service_id, replace(batch, rows=bad_rows), generation="g", store=store, client=client)
    store.claim.assert_not_called()
    client.insert_rows.assert_not_called()


def test_insert_failure_records_sanitized_failure_and_raises(monkeypatch):
    dataset, artifact, batch = fixture_manifest()
    store, client = MagicMock(), MagicMock()
    store.artifact.return_value = artifact
    store.claim.return_value = 1
    store.generation.return_value = {"target_identity": "target"}
    monkeypatch.setattr("backend.core.clickhouse_publication.target_identity", lambda c: "target")
    client.insert_rows.side_effect = RuntimeError("credentials must not escape")
    with pytest.raises(RuntimeError, match="publication failed: RuntimeError"):
        publish_batch(dataset.service_id, batch, generation="g", store=store, client=client)
    assert store.finish.call_args.kwargs["error"] == "RuntimeError" and store.finish.call_args.kwargs["digest"] is None


@pytest.fixture
def published_fixture(monkeypatch):
    dataset, artifact, batch = fixture_manifest()
    store, client = MagicMock(), MagicMock()
    store.artifact.return_value = artifact
    store.artifacts.return_value = [artifact]
    store.dataset.return_value = dataset
    store.generation.return_value = {"target_identity": "target", "dataset_id": dataset.dataset_id}
    store.claim.return_value = 1
    store.finish.return_value = True
    store.activate.return_value = True
    store.selected.return_value = {"dataset_id": dataset.dataset_id, "generation": "g"}
    monkeypatch.setattr("backend.core.clickhouse_publication.target_identity", lambda c: "target")

    def execute(sql, params):
        assert "log_facts FINAL" in sql and params["service"] == dataset.service_id
        if "SELECT count()" in sql:
            return [{"n": len(batch.rows)}]
        return [dict(zip(PAYLOAD_COLUMNS, r, strict=True)) for r in batch.rows]

    client.execute.side_effect = execute
    return dataset, artifact, batch, store, client


def test_successful_publication_checks_actual_ordered_payload(published_fixture):
    dataset, artifact, batch, store, client = published_fixture
    result = publish_batch(dataset.service_id, batch, generation="g", store=store, client=client)
    assert result.status == "published" and result.row_count == 2 and result.fence == 1
    assert store.finish.call_args.kwargs["digest"] == artifact.canonical_digest
    client.insert_rows.assert_called_once()
    store.claim.return_value = None
    assert publish_batch(dataset.service_id, batch, generation="g", store=store, client=client).status == "not_claimed"
    client.insert_rows.assert_called_once()


@pytest.mark.parametrize("outcome", ["published", "stale", "not_claimed", "failed"])
def test_publication_metric_includes_preclaim_errors(published_fixture, monkeypatch, outcome):
    dataset, _, batch, store, client = published_fixture
    record = MagicMock()
    monkeypatch.setattr("backend.core.clickhouse_publication.record_publication", record)
    if outcome == "not_claimed":
        store.claim.return_value = None
    elif outcome == "stale":
        store.finish.return_value = False
    elif outcome == "failed":
        store.artifact.side_effect = ValueError("manifest unavailable")
        with pytest.raises(ValueError, match="manifest unavailable"):
            publish_batch(dataset.service_id, batch, generation="g", store=store, client=client)
        store.claim.assert_not_called()
    if outcome != "failed":
        assert publish_batch(dataset.service_id, batch, generation="g", store=store, client=client).status == outcome
    record.assert_called_once_with(outcome)


def test_publication_telemetry_verification_and_rebuild(published_fixture, monkeypatch):
    dataset, artifact, batch, store, client = published_fixture
    monkeypatch.setattr(
        "backend.core.clickhouse_publication.record_publication", MagicMock(side_effect=RuntimeError("telemetry"))
    )
    monkeypatch.setattr("backend.core.clickhouse_publication.logger.info", MagicMock(side_effect=RuntimeError("log")))
    assert publish_batch(dataset.service_id, batch, generation="g", store=store, client=client).status == "published"
    client.insert_rows.side_effect = ValueError("original")
    store.finish.side_effect = RuntimeError("manifest unavailable")
    with pytest.raises(RuntimeError, match="publication failed: ValueError"):
        publish_batch(dataset.service_id, batch, generation="g", store=store, client=client)

    client.insert_rows.side_effect = None
    store.finish.side_effect = None
    assert verify_generation(dataset.service_id, "g", store=store, client=client) == dataset
    store.artifacts.return_value = [replace(artifact, ordinal_start=1)]
    with pytest.raises(ValueError, match="coverage"):
        verify_generation(dataset.service_id, "g", store=store, client=client)
    store.artifacts.return_value = [artifact]
    store.dataset.return_value = replace(dataset, canonical_digest="wrong")
    with pytest.raises(ValueError, match="digest"):
        verify_generation(dataset.service_id, "g", store=store, client=client)

    store.dataset.return_value = dataset
    store.begin_generation.return_value = "new-generation"
    store.pending.side_effect = [[artifact.batch_id], []]
    result = full_rebuild(dataset.service_id, dataset.dataset_id, loader=lambda a: batch, store=store, client=client)
    assert result.generation == "new-generation" and result.activated and result.published == 1
    store.begin_generation.assert_called_once_with(dataset.service_id, dataset.dataset_id, "target")
    store.pending.side_effect = [[artifact.batch_id], ["later"]]
    assert not replay_unpublished(
        dataset.service_id, generation="g", loader=lambda a: batch, store=store, client=client
    ).activated
    with pytest.raises(ValueError, match="limit"):
        full_rebuild(
            dataset.service_id, dataset.dataset_id, loader=lambda a: batch, limit=0, store=store, client=client
        )


@pytest.fixture
def settings():
    return config.ClickHouseConfig(
        host="localhost",
        port=8123,
        database="analytics",
        user="default",
        password="",
        secure=False,
        pool_max_size=1,
        pool_timeout_s=0.05,
    )


@pytest.fixture
def clients(settings):
    opened = []

    def build(handler):
        c = ch.ClickHouseClient(settings, transport=httpx.MockTransport(handler))
        opened.append(c)
        return c

    yield build
    for c in opened:
        c.close()


def response(rows=None):
    return httpx.Response(
        200,
        json={"data": rows if rows is not None else [{"ok": 1}], "statistics": {"rows_read": 7, "bytes_read": 81}},
    )


def test_allowlisted_tables_and_columns():
    for table in (
        "origin_minute_summary",
        "origin_minute_dimensions",
        "performance_minute_summary",
        "performance_scatter_samples",
    ):
        assert table in ch.CLICKHOUSE_HIGH_SCALE_TABLES
    for column in (
        "requests",
        "misses",
        "passes",
        "origin_5xx",
        "origin_bytes",
        "latency_count",
        "latency_p50_us",
        "latency_p75_us",
        "latency_p95_us",
        "latency_p99_us",
        "ttlb_p50_us",
        "ttlb_p95_us",
        "cdn_overhead_p50_us",
        "origin_bytes_p50",
        "latency_sk_v",
        "latency_sk_w",
        "ttlb_sk_v",
        "ttlb_sk_w",
        "overhead_sk_v",
        "overhead_sk_w",
        "origin_bytes_sk_v",
        "origin_bytes_sk_w",
        "component_count",
        "edge_processing_sum_ms",
        "origin_wait_sum_ms",
        "origin_download_sum_ms",
        "client_download_sum_ms",
        "sample_key",
        "origin_ms",
        "edge_ms",
        "cache",
    ):
        assert column in ch.CLICKHOUSE_HIGH_SCALE_COLUMNS
    assert {"country", "asn", "cmcd"} <= ch.CLICKHOUSE_TABLE_COLUMNS["cmcd_projection_facts"]
    environment = {"browser", "os", "device", "city", "region", "pop", "tls", "ttfb", "country"}
    assert environment <= ch.CLICKHOUSE_TABLE_COLUMNS["rum_vitals_facts"]
    assert environment | {"error_line", "error_col"} <= ch.CLICKHOUSE_TABLE_COLUMNS["rum_error_facts"]


def query_form(request):
    assert request.headers["content-type"].startswith("multipart/form-data;")
    message = BytesParser(policy=default).parsebytes(
        b"Content-Type: " + request.headers["content-type"].encode() + b"\r\n\r\n" + request.content
    )
    return {
        part.get_param("name", header="content-disposition"): [part.get_payload(decode=True).decode()]
        for part in message.iter_parts()
    }


def test_disabled_never_constructs_http_client(monkeypatch):
    monkeypatch.delenv("CLICKHOUSE_ENABLED", raising=False)
    monkeypatch.setattr(ch.httpx, "Client", lambda **_: pytest.fail("opened HTTP client"))
    assert ch.get_clickhouse_client() is None


def test_parameters_are_bound_not_interpolated_or_in_url(clients):
    value = "x' OR 1=1 --\n\t\\N"
    sql = "SELECT {country:String} AS country, {limit:UInt64} AS n"
    seen = []

    def handler(request):
        seen.append(request)
        form = query_form(request)
        assert form["query"] == [sql]
        assert form["param_country"] == ["x' OR 1=1 --\\n\\t\\\\N"] and form["param_limit"] == ["2"]
        assert "param_country" not in request.url.params and value not in str(request.url)
        assert request.url.params["default_format"] == "JSON" and float(request.url.params["max_execution_time"]) > 0
        assert request.url.params["wait_end_of_query"] == "1" and request.url.params["max_memory_usage"] == "1073741824"
        assert request.url.params["max_threads"] == "2"
        assert request.url.params["max_bytes_before_external_group_by"] == "134217728"
        assert request.url.params["max_bytes_before_external_sort"] == "134217728"
        UUID(request.url.params["query_id"])
        return response([{"country": value, "n": 2}])

    c = clients(handler)
    assert c.execute(sql, {"country": value, "limit": 2}) == [{"country": value, "n": 2}]
    c.execute(sql, {"country": value, "limit": 2})
    assert seen[0].url.params["query_id"] != seen[1].url.params["query_id"]


def test_internal_ddl_and_insert_and_delete(clients, settings):
    sql = f"CREATE TABLE IF NOT EXISTS {ch.CLICKHOUSE_FACT_TABLE} (service_id String) ENGINE = Memory"
    assert (
        clients(lambda req: httpx.Response(200, content=b"") if query_form(req)["query"] == [sql] else None).execute(
            sql
        )
        == []
    )

    columns = [
        "service_id",
        "batch_id",
        "row_ordinal",
        "generation",
        "timestamp",
        "country",
        "ip",
        "url",
        "conn_requests",
    ]
    r = (
        "test-service",
        "batch-1",
        0,
        1,
        datetime(2026, 9, 7, tzinfo=UTC),
        "US",
        "192.0.2.1",
        "'; DROP TABLE logs; --\n",
        3,
    )

    def handler(request):
        assert request.url.params["query"] == (
            f"INSERT INTO `{ch.CLICKHOUSE_FACT_TABLE}` ({', '.join(f'`{c}`' for c in columns)}) FORMAT JSONCompactEachRow"
        )
        assert request.url.params["async_insert"] == "0" and request.url.params["wait_end_of_query"] == "1"
        assert float(request.url.params["max_execution_time"]) == settings.insert_timeout_s
        assert request.url.params["max_memory_usage"] == "536870912" and request.url.params["max_threads"] == "2"
        assert request.url.params["max_insert_threads"] == "1" and request.url.params["max_block_size"] == "2048"
        body = json.loads(request.content)
        assert body[:4] == list(r[:4]) and body[4] == "2026-09-07 00:00:00.000000" and body[5:] == list(r[5:])
        return httpx.Response(200)

    assert clients(handler).insert_rows(ch.CLICKHOUSE_FACT_TABLE, columns, [r]) is None
    clients(lambda _: pytest.fail("HTTP called for empty batch")).insert_rows(ch.CLICKHOUSE_FACT_TABLE, ["country"], [])

    seen = {}
    clients(lambda req: seen.update(query_form(req)) or httpx.Response(200, content=b"")).delete_batch_rows(
        "request_facts", "123e4567-e89b-12d3-a456-426614174000"
    )
    assert seen["query"] == [
        "ALTER TABLE `request_facts` DELETE WHERE batch_id={batch_id:UUID} SETTINGS mutations_sync=2"
    ]
    assert seen["param_batch_id"] == ["123e4567-e89b-12d3-a456-426614174000"]


@pytest.mark.parametrize("table", ["arbitrary_table", "system.users", "logs; DROP TABLE logs", "`logs`", ""])
def test_insert_rejects_non_allowlisted_tables(clients, table):
    with pytest.raises(ValueError, match="table"):
        clients(lambda _: pytest.fail("HTTP called")).insert_rows(table, ["country"], [("US",)])


@pytest.mark.parametrize(
    "columns,rows",
    [
        (["not_internal"], [("US",)]),
        (["country); DROP TABLE logs"], [("US",)]),
        ([], [()]),
        (["country", "country"], [("US", "US")]),
        (["country"], [("US", 1)]),
        (["conn_requests"], [(float("nan"),)]),
    ],
)
def test_invalid_insert_never_reaches_transport(clients, columns, rows):
    with pytest.raises(ValueError):
        clients(lambda _: pytest.fail("HTTP called")).insert_rows(ch.CLICKHOUSE_FACT_TABLE, columns, rows)


@pytest.mark.parametrize("failure", ["timeout", "connect", "server", "redirect", "invalid_json"])
def test_errors_are_explicit_sanitized_and_never_retried(clients, failure, caplog):
    calls = []
    secret = "FAKE_PASSWORD raw SQL private parameter"

    def handler(request):
        calls.append(request)
        if failure == "timeout":
            raise httpx.ReadTimeout(secret, request=request)
        if failure == "connect":
            raise httpx.ConnectError(secret, request=request)
        if failure == "server":
            return httpx.Response(503, text=secret)
        if failure == "redirect":
            return httpx.Response(307, headers={"location": "https://example.com/"}, text=secret)
        return httpx.Response(200, text=secret)

    with pytest.raises(ch.ClickHouseError) as error:
        clients(handler).execute("SELECT 1")
    assert len(calls) == 1
    assert secret not in "".join(traceback.format_exception(error.value)) and secret not in caplog.text
    assert error.value.query_id
    if failure == "server":
        assert error.value.status_code == 503


def test_insert_timeout_and_health_and_storage_health(clients):
    calls = []

    def timeout_handler(req):
        calls.append(req)
        raise httpx.ReadTimeout("ambiguous write", request=req)

    with pytest.raises(ch.ClickHouseError, match="timeout"):
        clients(timeout_handler).insert_rows(ch.CLICKHOUSE_FACT_TABLE, ["country"], [("US",)])
    assert len(calls) == 1

    assert clients(lambda _: response()).health()["ok"] is True
    with pytest.raises(ch.ClickHouseError):
        clients(lambda _: httpx.Response(503)).health()

    def disk_handler(request):
        sql = query_form(request)["query"][0]
        assert (
            "system.disks" in sql
            and "name = 'default'" in sql
            and request.headers["authorization"].startswith("Basic ")
        )
        return response([{"free_space": 40, "total_space": 100}])

    assert clients(disk_handler).storage_health() == {"up": 1, "disk_free": 40, "disk_total": 100}
    with pytest.raises(ch.ClickHouseError):
        clients(lambda _: httpx.Response(503)).storage_health()
    with pytest.raises(ValueError):
        clients(lambda _: response([])).storage_health()


def test_active_query_monitor_pool_acquisition_and_close(clients):
    from backend.core.query_registry import query_registry

    active = []

    def handler(_):
        entries = [q for q in query_registry._queries.values() if q.db_type == "ClickHouse"]
        assert len(entries) == 1 and entries[0]._con_id is None and "PRIVATE" not in entries[0].sql
        active.append(entries[0].query_id)
        return response()

    clients(handler).execute("SELECT {secret:String}", {"secret": "PRIVATE"})
    assert active and active[0] not in query_registry._queries

    entered, release = threading.Event(), threading.Event()
    c = clients(lambda _: (entered.set(), assert_true := release.wait(3), response())[2])
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(c.execute, "SELECT 1")
        try:
            assert entered.wait(3)
            with pytest.raises(ch.ClickHouseError, match="pool"):
                c.execute("SELECT 1")
        finally:
            release.set()
        assert future.result() == [{"ok": 1}]
    assert c.execute("SELECT 1") == [{"ok": 1}]

    c.close()
    c.close()
    with pytest.raises(ch.ClickHouseError, match="closed"):
        c.execute("SELECT 1")


def test_statistics_and_client_phases_and_section_identity(clients, monkeypatch):
    from backend.core.high_scale_sections import HighScaleSections
    from backend.core.query_attribution import Attribution, current_attribution

    events = []
    monkeypatch.setattr(ch.logger, "info", lambda event, **kw: events.append((event, kw)))
    telemetry.start_call_tracking()
    assert clients(lambda _: response()).execute("SELECT {secret:String}", {"secret": "PRIVATE"}) == [{"ok": 1}]
    event, stats = events[-1]
    assert event == "clickhouse.operation" and stats["rows_read"] == 7 and stats["bytes_read"] == 81
    assert "PRIVATE" not in str(events) and telemetry.get_tracked_calls()[-1]["service"] == "ClickHouse"

    clock, captured, registered, transmitted = [0.0], [], [], []
    monkeypatch.setattr(ch.time, "perf_counter", lambda: clock[0])
    monkeypatch.setattr(ch, "_record_operation", lambda **s: captured.append(s))
    monkeypatch.setattr(ch.query_registry, "register", lambda *a, **kw: registered.append(kw) or -1)

    class Slots:
        def acquire(self, **kwargs):
            clock[0] += 0.02
            return True

        def release(self):
            pass

    def handler(request):
        transmitted.append(request.url.params["query_id"])
        clock[0] += 0.05
        res = response()
        decode = res.json

        def json_body():
            clock[0] += 0.03
            payload = decode()
            payload["statistics"]["elapsed"] = 0.004
            return payload

        res.json = json_body
        return res

    c = clients(handler)
    c._slots = Slots()
    token = current_attribution.set(Attribution(kind="admin", request_id="request-test"))
    try:
        sections = HighScaleSections("service-test", "dashboard")
        res = sections.run("summary", lambda: c.execute("SELECT 1"), source="projection")
    finally:
        current_attribution.reset(token)
    assert res == [{"ok": 1}]
    st = captured[0]
    assert st["service_id"] == registered[0]["service_id"] == "service-test"
    assert st["request_id"] == "request-test" and st["section"] == "dashboard.summary"
    assert st["admission_wait_ms"] == pytest.approx(20) and st["network_ms"] == pytest.approx(50)
    assert st["decode_ms"] == pytest.approx(30) and st["client_elapsed_ms"] == pytest.approx(100)
    assert st["server_elapsed_ms"] == pytest.approx(4) and st["query_id"] == transmitted[0]
    obs = DebugQuery.model_validate(telemetry.get_queries()[-1]).clickhouse
    assert obs.query_id == transmitted[0] and obs.server_elapsed_ms == pytest.approx(4)


@pytest.mark.parametrize("value", [None, True, "1", -1, float("nan"), float("inf"), 10**500])
def test_server_elapsed_validation(value):
    assert ch._server_elapsed_ms(value) is None
    assert ch._server_elapsed_ms(0) == 0 and ch._server_elapsed_ms(0.005) == pytest.approx(5)


def test_pool_rejection_and_telemetry_resilience(clients, monkeypatch):
    clock, captured = [0.0], []
    monkeypatch.setattr(ch.time, "perf_counter", lambda: clock[0])
    monkeypatch.setattr(ch, "_record_operation", lambda **s: captured.append(s))

    class Slots:
        def acquire(self, **kwargs):
            clock[0] += 0.02
            return False

        def release(self):
            raise AssertionError("unacquired slot must not be released")

    c = clients(lambda req: pytest.fail("network must not be attempted"))
    c._slots = Slots()
    with pytest.raises(ch.ClickHouseError, match="timeout"):
        c.execute("SELECT 1")
    assert captured[0]["admission_wait_ms"] == pytest.approx(20) and captured[0]["network_ms"] is None

    monkeypatch.setattr(ch, "_record_operation", MagicMock(side_effect=RuntimeError("telemetry failed")))
    assert clients(lambda _: response()).execute("SELECT 1") == [{"ok": 1}]


@pytest.mark.parametrize("body", [b"", b'{"data": {}}', b'{"data": [1]}', b'{"data": []}\nCode: 1, secret'])
def test_invalid_select_response_and_exception_header_and_identifier_binding(clients, body):
    with pytest.raises(ch.ClickHouseError):
        clients(lambda _: httpx.Response(200, content=body)).execute("SELECT 1")
    with pytest.raises(ch.ClickHouseError, match="server error"):
        clients(lambda _: httpx.Response(200, headers={"X-ClickHouse-Exception-Code": "159"})).execute("SELECT 1")
    with pytest.raises(ValueError, match="identifier"):
        clients(lambda _: pytest.fail("request attempted")).execute(
            "SELECT * FROM {table:Identifier}",
            {"table": ch.CLICKHOUSE_FACT_TABLE},
        )


@pytest.mark.parametrize(
    "value,encoded",
    [
        (None, r"\N"),
        (r"\N", r"\\N"),
        (True, "1"),
        (datetime(2026, 9, 7, tzinfo=UTC), "2026-09-07 00:00:00.000000"),
        ("ü\n", "ü\\n"),
    ],
)
def test_typed_scalar_parameters(clients, value, encoded):
    clients(lambda req: response() if query_form(req)["param_value"] == [encoded] else None).execute(
        "SELECT {value:Nullable(String)}", {"value": value}
    )


def test_pool_config_close_during_request_and_singleton(settings, monkeypatch):
    constructor = MagicMock()
    with patch.object(ch.httpx, "Client", constructor):
        c = ch.ClickHouseClient(settings)
        kw = constructor.call_args.kwargs
        assert (
            kw["limits"].max_connections == settings.pool_max_size
            and kw["follow_redirects"] is False
            and kw["trust_env"] is False
        )
        c.close()

    entered, release = threading.Event(), threading.Event()

    class Transport(httpx.MockTransport):
        closed = False

        def close(self):
            self.closed = True

    transport = Transport(lambda _: (entered.set(), release.wait(3), response())[2])
    c2 = ch.ClickHouseClient(settings, transport=transport)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(c2.execute, "SELECT 1")
        try:
            assert entered.wait(3)
            c2.close()
            assert not transport.closed
            with pytest.raises(ch.ClickHouseError, match="closed"):
                c2.execute("SELECT 1")
        finally:
            release.set()
        assert future.result() == [{"ok": 1}]
    assert transport.closed

    monkeypatch.setattr(ch, "_client", None)
    monkeypatch.setattr(ch.config, "load_clickhouse_config", lambda: settings)
    ctor = MagicMock(return_value=MagicMock())
    monkeypatch.setattr(ch, "ClickHouseClient", ctor)
    with ThreadPoolExecutor(max_workers=4) as executor:
        instances = list(executor.map(lambda _: ch.get_clickhouse_client(), range(8)))
    assert all(i is instances[0] for i in instances) and ctor.call_count == 1
    ch.close_clickhouse_client()
    instances[0].close.assert_called_once()
    assert ch._client is None


@pytest.mark.parametrize("failure", [None, "health", "startup", "serving"])
def test_app_lifecycle_initializes_probes_and_always_closes(monkeypatch, failure):
    from backend import main

    c = MagicMock()
    if failure == "health":
        c.health.side_effect = ch.ClickHouseError("server error", query_id="test")
    monkeypatch.setattr(ch, "_client", None)
    with (
        patch("backend.main._enforce_data_dir_mounted"),
        patch("backend.main._migrate_config_on_startup"),
        patch("backend.main._enforce_proxy_headers_configured"),
        patch("backend.core.metadata.pg_schema.ensure_pg_schema"),
        patch("backend.config.validate_deployment_mode"),
        patch("backend.utils.telemetry_proxy.start_proxy_server"),
        patch("backend.utils.tunnel.get_tunnel_manager"),
        patch("backend.main._bounded_scheduler_shutdown"),
        patch("backend.core.duckdb.close_all_connections"),
        patch("backend.core.clickhouse_client.get_clickhouse_client", return_value=c) as initialize,
        patch("backend.core.clickhouse_client.close_clickhouse_client") as close,
    ):

        async def run_lifespan():
            async with main.lifespan(main.app):
                initialize.assert_called_once()
                c.health.assert_called_once()
                if failure == "serving":
                    raise RuntimeError("serving failure")

        if failure == "startup":
            monkeypatch.setattr(
                main, "register_high_scale_services_from_environment", MagicMock(side_effect=RuntimeError("startup"))
            )
        if failure:
            with pytest.raises((RuntimeError, ch.ClickHouseError)):
                asyncio.run(run_lifespan())
        else:
            asyncio.run(run_lifespan())
        close.assert_called_once()


def test_close_wraps_transport_failure_and_otel_metrics(settings, clients, monkeypatch):
    class Transport(httpx.MockTransport):
        def close(self):
            raise httpx.CloseError("FAKE_PASSWORD in raw close error")

    c = ch.ClickHouseClient(settings, transport=Transport(lambda _: response()))
    with pytest.raises(ch.ClickHouseError, match="close") as error:
        c.close()
    assert "FAKE_PASSWORD" not in "".join(traceback.format_exception(error.value))
    c.close()

    clickhouse_metrics._instrument.cache_clear()
    meter = MagicMock()
    inst = {}
    meter.create_histogram.side_effect = lambda name, **kw: inst.setdefault(name, MagicMock())
    monkeypatch.setattr(clickhouse_metrics, "get_meter", lambda: meter)
    try:
        cl = clients(lambda _: response())
        cl.execute("SELECT 1")
        cl.execute("SELECT 1")
        assert meter.create_histogram.call_count == 4
        assert set(inst) == {
            "app.clickhouse_query_duration_ms",
            "app.clickhouse_admission_wait_ms",
            "app.clickhouse_network_ms",
            "app.clickhouse_decode_ms",
        }
    finally:
        clickhouse_metrics._instrument.cache_clear()


def test_real_http_transport_posts_multipart_with_auth_and_json_insert(settings):
    observed = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            observed.append(
                {
                    "path": self.path,
                    "headers": dict(self.headers),
                    "body": self.rfile.read(int(self.headers["Content-Length"])),
                }
            )
            body = b'{"data":[{"ok":1}]}' if len(observed) == 1 else b""
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    c = ch.ClickHouseClient(replace(settings, host="127.0.0.1", port=server.server_port))
    try:
        assert c.execute("SELECT {value:UInt8} AS ok", {"value": 1}) == [{"ok": 1}]
        c.insert_rows(ch.CLICKHOUSE_FACT_TABLE, ["country"], [("US",)])
        assert observed[0]["headers"]["Content-Type"].startswith("multipart/form-data;")
        assert observed[0]["headers"]["Authorization"].startswith("Basic ")
        assert b'name="param_value"' in observed[0]["body"] and "param_value" not in observed[0]["path"]
        assert json.loads(observed[1]["body"]) == ["US"]
    finally:
        c.close()
        server.shutdown()
        server.server_close()
        worker.join(timeout=3)
    assert not worker.is_alive()


@pytest.mark.parametrize("operation", ["execute", "insert"])
def test_per_operation_timeouts_and_server_deadline(settings, operation):
    settings = replace(settings, connect_timeout_s=2, query_timeout_s=4, insert_timeout_s=8)
    expected = 8 if operation == "insert" else 4

    def handler(request):
        assert request.extensions["timeout"] == {"connect": 2, "pool": 0.05, "read": expected, "write": expected}
        assert float(request.url.params["max_execution_time"]) == expected
        return httpx.Response(200) if operation == "insert" else response()

    c = ch.ClickHouseClient(settings, transport=httpx.MockTransport(handler))
    try:
        if operation == "insert":
            c.insert_rows(ch.CLICKHOUSE_FACT_TABLE, ["country"], [("US",)])
        else:
            c.execute("SELECT 1")
    finally:
        c.close()


@pytest.mark.parametrize("header", ['{"read_rows":"10","read_bytes":"100"}', "invalid", "null"])
def test_summary_headers_and_settings_and_credential_redaction(clients, monkeypatch, header):
    recorded = []
    monkeypatch.setattr(ch, "_record_operation", lambda **stats: recorded.append(stats))
    clients(lambda _: httpx.Response(200, headers={"X-ClickHouse-Summary": header})).insert_rows(
        ch.CLICKHOUSE_FACT_TABLE, ["country"], [("US",)]
    )
    assert recorded[-1]["rows_written"] == 1
    if header.startswith("{"):
        assert recorded[-1]["rows_read"] == 10 and recorded[-1]["bytes_read"] == 100


def test_query_settings_and_credential_redaction(clients):
    filters = "{'request_facts': 'service_id = \\'svc\\''}"
    seen = []
    clients(lambda req: (seen.append(req), response([{"n": 1}]))[1]).execute(
        "SELECT 1 AS n",
        settings={"additional_table_filters": filters, "max_result_rows": "11", "result_overflow_mode": "break"},
    )
    params = seen[0].url.params
    assert params["additional_table_filters"] == filters and params["max_result_rows"] == "11"

    for name in ("readonly", "max_memory_usage", "max_execution_time", "query_id", "database", "not_a_setting"):
        with pytest.raises(ValueError, match="setting"):
            clients(lambda req: pytest.fail("rejected")).execute("SELECT 1", settings={name: "1"})

    token = telemetry._QUERIES.set([])
    try:
        clients(lambda req: httpx.Response(200, content=b"")).execute(
            "ALTER USER `u` IDENTIFIED WITH sha256_password BY 'hunter2-super-secret' DEFAULT DATABASE `d`"
        )
        recorded = " ".join(q["sql"] for q in telemetry.get_queries())
    finally:
        telemetry._QUERIES.reset(token)
    assert "hunter2-super-secret" not in recorded and "BY '[REDACTED]'" in recorded


@pytest.fixture
def instruments(monkeypatch):
    metrics._instrument.cache_clear()
    meter, found = MagicMock(), {}

    def create(name, **kwargs):
        found[name] = (MagicMock(), kwargs)
        return found[name][0]

    meter.create_counter.side_effect = create
    meter.create_histogram.side_effect = create
    monkeypatch.setattr(metrics, "get_meter", lambda: meter)
    yield found
    metrics._instrument.cache_clear()


def client(handler):
    return ClickHouseClient(
        config.ClickHouseConfig(
            host="localhost", port=8123, database="analytics", user="default", password="", secure=False
        ),
        transport=httpx.MockTransport(handler),
    )


def test_lazy_query_and_insert_instruments_and_timeout(instruments, monkeypatch):
    con = client(lambda _: httpx.Response(200, json={"data": [], "statistics": {"bytes_read": 81}}))
    try:
        con.execute("SELECT {value:String}", {"value": "PRIVATE"})
    finally:
        con.close()
    assert "app.clickhouse_queries_total" in instruments and "PRIVATE" not in str(instruments)
    instruments["app.clickhouse_bytes_read"][0].add.assert_called_once_with(81)

    replies = iter([httpx.Response(200), httpx.Response(200), httpx.Response(503)])
    con2 = client(lambda _: next(replies))
    try:
        for _ in range(2):
            con2.insert_rows("log_facts", ["country"], [("US",), ("US",)])
        with pytest.raises(ClickHouseError):
            con2.insert_rows("log_facts", ["country"], [("US",)])
    finally:
        con2.close()
    assert [c.args for c in instruments["app.clickhouse_rows_inserted_total"][0].add.call_args_list] == [(2,), (2,)]

    con3 = client(lambda _: pytest.fail("admission failed"))
    con3._slots = MagicMock()
    con3._slots.acquire.return_value = False
    try:
        with pytest.raises(ClickHouseError, match="pool"):
            con3.execute("SELECT 1")
    finally:
        con3.close()
    instruments["app.clickhouse_queries_total"][0].add.assert_any_call(1, {"outcome": "timeout"})

    monkeypatch.setattr(metrics, "_instrument", MagicMock(side_effect=RuntimeError("telemetry broken")))
    con4 = client(lambda _: httpx.Response(503))
    try:
        with pytest.raises(ClickHouseError, match="server error"):
            con4.execute("SELECT 1")
    finally:
        con4.close()


def test_observer_gauges_errors_and_stop(monkeypatch):
    cl, store, meter = MagicMock(), MagicMock(), MagicMock()
    cl.storage_health.return_value = {"up": 1, "disk_free": 40, "disk_total": 100}
    store.publication_lag_seconds.return_value = 12.5
    monkeypatch.setattr(observer, "get_meter", lambda: meter)
    monkeypatch.setattr(observer, "PgManifest", lambda: store)
    sampler = observer.ClickHouseObserver()
    sampler.start(cl)
    try:
        assert sampler.observe("up")[0].value == 1 and sampler.observe("disk_free")[0].value == 40
        assert sampler.observe("lag")[0].value == 12.5
        cl.storage_health.assert_called_once_with()

        cl.storage_health.side_effect = RuntimeError("PRIVATE")
        store.publication_lag_seconds.side_effect = RuntimeError("PRIVATE")
        sampler._sampled_at = 0
        assert (
            sampler.observe("up")[0].value == 0 and sampler.observe("disk_free") == [] and sampler.observe("lag") == []
        )

        sampler.stop()
        sampler.stop()
        assert sampler.observe("up") == []
        cl.close.assert_called_once_with()
    finally:
        sampler.stop()

    monkeypatch.setattr(observer.config, "load_clickhouse_config", lambda: None)
    make = MagicMock(side_effect=AssertionError("opened disabled client"))
    monkeypatch.setattr(observer, "ClickHouseClient", make)
    observer.start_clickhouse_observer()
    make.assert_not_called()


def _rows_from_fixture(con) -> list[tuple]:
    out = []
    for idx, r in enumerate(con.execute("SELECT * FROM fixture").fetch_arrow_table().to_pylist()):
        r["row_ordinal"] = idx
        out.append(canonical_row(r))
    return out


@pytest.fixture
def fixture():
    source = {"service_id": "TestPrototype", "name": "TestPrototype", "bucket": "test-bucket", "prefix": "logs"}
    fos = TinyFos()
    objects = FosArtifacts(source, client=fos)
    dataset = Dataset(
        source["service_id"],
        "test-dataset",
        "test-catalog",
        "logs_fixture",
        1,
        datetime(2026, 9, 1, tzinfo=UTC),
        datetime(2026, 9, 2, tzinfo=UTC),
        datetime.now(UTC) + timedelta(hours=1),
        0,
        digest_rows(()),
    )
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE fixture AS SELECT 'source' AS source_identity, "
        "'2026-09-01 00:00:00'::TIMESTAMP AS timestamp, NULL::VARCHAR AS country, "
        "'192.0.2.1' AS ip, '/' AS url, 1::BIGINT AS conn_requests FROM range(5)"
    )
    yield dataset, objects, fos, con
    con.close()


def test_fos_artifacts_roundtrip_corrupt_and_head_mismatch(fixture):
    dataset, objects, fos, con = fixture
    all_rows = _rows_from_fixture(con)
    artifacts = [objects.put(dataset, tuple(all_rows[i : i + 2])) for i in range(0, len(all_rows), 2)]
    rows = tuple(r for a in artifacts for r in objects.load(a).rows)
    assert [r[1] for r in rows] == list(range(5)) and digest_rows(rows) == digest_rows(tuple(all_rows))
    assert len(artifacts) == 3 and fos.puts == 3

    artifact = artifacts[0]
    with pytest.raises(ValueError, match="bucket"):
        objects.load(replace(artifact, artifact_uri=artifact.artifact_uri.replace("test-bucket", "other-bucket")))
    key = next(iter(fos.objects))
    payload, metadata = fos.objects[key]
    fos.objects[key] = (b"x" + payload[1:], metadata)
    with pytest.raises(ValueError, match="hash"):
        objects.load(artifact)

    fos.head_object = lambda **kw: {"ContentLength": 0, "Metadata": {}}
    with pytest.raises(ValueError, match="HEAD"):
        objects.put(dataset, tuple(all_rows))


@pytest.mark.parametrize(
    "mode",
    [
        ["--export", "--start", "2026-09-01", "--end", "2026-09-02"],
        ["--rebuild", "dataset"],
        ["--resume", "generation"],
    ],
)
def test_cli_dryrun_and_validation_and_flush(monkeypatch, mode):
    no_storage = MagicMock(side_effect=AssertionError("must not open storage"))
    for name in ("PgManifest", "FosArtifacts"):
        monkeypatch.setattr(f"scripts.clickhouse_replay.{name}", no_storage)
    assert run(parser().parse_args(["--service-id", "TestPrototype", *mode]))["dry_run"]

    for extra in (["--limit", "0"], ["--max-rows", "1000001"], ["--ttl-hours", "nan"]):
        with pytest.raises(ValueError):
            run(
                parser().parse_args(
                    [
                        "--service-id",
                        "TestPrototype",
                        "--export",
                        "--start",
                        "2026-09-01",
                        "--end",
                        "2026-09-02",
                        *extra,
                    ]
                )
            )

    events = []
    monkeypatch.setattr("sys.argv", ["replay", "--service-id", "TestPrototype", "--rebuild", "fixture"])
    monkeypatch.setattr(clickhouse_replay, "force_flush", lambda: events.append("flush"))
    monkeypatch.setattr(clickhouse_replay, "close_clickhouse_client", lambda: events.append("close"))
    monkeypatch.setattr(clickhouse_replay, "run", lambda args: {"dry_run": True})
    assert clickhouse_replay.main() == 0 and events == ["flush", "close"]
    events.clear()
    monkeypatch.setattr(clickhouse_replay, "run", MagicMock(side_effect=ValueError("PRIVATE")))
    assert clickhouse_replay.main() == 1 and events == ["flush", "close"]


@unittest.skipUnless(os.getenv("RUN_CLICKHOUSE_INTEGRATION") == "1", "explicit real-engine gate")
class PublicationIntegration(unittest.TestCase):
    pass


@pytest.fixture
def manifest(monkeypatch):
    monkeypatch.setenv("METADATA_DSN", "postgresql://placeholder")
    pool, con = MagicMock(), MagicMock()
    pool.connection.return_value.__enter__.return_value = con
    monkeypatch.setattr("backend.core.clickhouse_manifest.get_pg_pool", lambda: pool)
    return PgManifest(), con


def result(con, row=None, rows=None, count=1):
    cursor = MagicMock()
    cursor.fetchone.return_value = row
    cursor.fetchall.return_value = rows or []
    cursor.rowcount = count
    con.execute.return_value = cursor


def test_admin_manifest_transactions_and_status(manifest):
    store, con = manifest
    store.bounded = True
    with store.transaction():
        pass
    store.pool.connection.assert_called_once_with(timeout=2)
    assert [c.args[0] for c in con.execute.call_args_list] == [
        "SET LOCAL statement_timeout = '2s'",
        "SET LOCAL lock_timeout = '2s'",
    ]

    now = datetime.now(UTC)
    rows = iter(
        [
            {"schema_version": 1},
            {"pending_age": 23, "last_published_at": now},
            {
                "generation": "g",
                "dataset_id": "d",
                "target_identity": "old-target",
                "coverage_start": now,
                "coverage_end": now,
                "expires_at": now,
                "expired": True,
                "coverage_age_seconds": 25,
            },
            {"n": 1},
        ]
    )
    con.execute.return_value.fetchone.side_effect = lambda: next(rows)
    con.execute.return_value.fetchall.return_value = [{"status": "failed", "n": 2}]
    status = store.admin_status("service", "target")
    assert status["schema_version"] == 1 and status["publication_counts"]["failed"] == 2
    assert status["active_generation"]["expired"] is True and "target_identity" not in status["active_generation"]


@pytest.mark.parametrize("generation", [None, "g"])
def test_replay_preview_only_reads_and_checks_service_references(manifest, generation):
    store, con = manifest
    dataset, _, _ = fixture_manifest()
    rows = (
        [{"dataset_id": "dataset", "status": "building", "target_identity": "target", "current_revision": True}]
        if generation
        else []
    )
    rows.extend([{"expires_at": dataset.expires_at, "schema_version": CLICKHOUSE_SCHEMA_VERSION}, {"n": 3}])
    if generation:
        rows.append({"n": 2})
    con.execute.return_value.fetchone.side_effect = rows
    assert store.replay_preview(
        "service", dataset_id=None if generation else "dataset", generation=generation, target="target", limit=1
    ) == {
        "dataset_id": "dataset",
        "artifact_count": 3,
        "planned_artifacts": 1,
    }


@pytest.mark.parametrize("case", ["missing", "expired", "schema", "oversized", "retired", "superseded", "target"])
def test_replay_preview_rejects_unreplayable_references(manifest, case):
    store, con = manifest
    dataset, _, _ = fixture_manifest()
    generation = "g" if case in ("retired", "superseded", "target") else None
    if generation:
        rows = [
            {
                "dataset_id": "dataset",
                "status": "retired" if case == "retired" else "building",
                "current_revision": case != "superseded",
                "target_identity": "other" if case == "target" else "target",
            }
        ]
    elif case == "missing":
        rows = [None]
    else:
        rows = [
            {
                "expires_at": datetime.now(UTC) - timedelta(seconds=1) if case == "expired" else dataset.expires_at,
                "schema_version": 99 if case == "schema" else CLICKHOUSE_SCHEMA_VERSION,
            },
            {"n": 101},
        ]
    con.execute.return_value.fetchone.side_effect = rows
    with pytest.raises(LookupError if case == "missing" else ValueError):
        store.replay_preview(
            "service", dataset_id=None if generation else "dataset", generation=generation, target="target", limit=100
        )


def test_manifest_lag_dataset_generation_claim_and_pending(manifest, monkeypatch):
    store, con = manifest
    result(con, {"lag_seconds": 42.5})
    assert store.publication_lag_seconds() == 42.5
    result(con, {"lag_seconds": None})
    assert store.publication_lag_seconds() == 0

    dataset, artifact, _ = fixture_manifest()
    result(con, asdict(dataset))
    assert store.dataset(dataset.service_id, dataset.dataset_id) == dataset
    result(con)
    with pytest.raises(ValueError, match="not found"):
        store.dataset(dataset.service_id, dataset.dataset_id)
    result(con, asdict(replace(dataset, expires_at=datetime.now(UTC) - timedelta(seconds=1))))
    with pytest.raises(ValueError, match="expired"):
        store.dataset(dataset.service_id, dataset.dataset_id)
    result(con, asdict(replace(dataset, schema_version=99)))
    with pytest.raises(ValueError, match="unsupported"):
        store.dataset(dataset.service_id, dataset.dataset_id)

    result(con, {"dataset_id": dataset.dataset_id})
    assert store.generation(dataset.service_id, "generation") == {"dataset_id": dataset.dataset_id}
    monkeypatch.setattr(store, "generation", lambda *a: {"dataset_id": dataset.dataset_id})
    monkeypatch.setattr(store, "dataset", lambda *a: dataset)
    result(con, asdict(artifact))
    assert store.artifact(dataset.service_id, "generation", artifact.batch_id) == artifact
    result(con, rows=[asdict(artifact)])
    assert store.artifacts(dataset.service_id, dataset.dataset_id) == [artifact]

    result(con, {"revision": 4})
    gen = store.begin_generation(dataset.service_id, dataset.dataset_id, "target")
    assert len(gen) == 32

    result(con, {"lease_fence": 3})
    assert store.claim("service", "generation", "batch") == 3
    with pytest.raises(ValueError):
        store.claim("service", "generation", "batch", lease_seconds=301)
    assert store.finish("service", "generation", "batch", 3, digest="digest")
    result(con, count=0)
    assert not store.finish("service", "generation", "batch", 2, digest=None, error="TimeoutError")

    result(con, rows=[{"batch_id": "b"}])
    assert store.pending("s", "g", 1) == ["b"]
    with pytest.raises(ValueError):
        store.pending("s", "g", 1001)
    result(con)
    assert store.selected("s") is None


@pytest.mark.parametrize(
    "scenario,activated", [("valid", True), ("missing", False), ("revision", False), ("pending", False)]
)
def test_activation_requires_complete_membership_locked_revision_and_live_target(manifest, scenario, activated):
    store, con = manifest
    dataset, _, _ = fixture_manifest()
    gen = {
        "status": "building",
        "target_identity": "target",
        "selection_revision": 2 if scenario == "revision" else 3,
        "expires_at": dataset.expires_at,
        "dataset_id": dataset.dataset_id,
    }
    rows = [None if scenario == "missing" else {"revision": 3}, gen, {"n": int(scenario == "pending")}]

    def execute(sql, params):
        cur = MagicMock()
        if sql.startswith("SELECT"):
            cur.fetchone.return_value = rows.pop(0)
        return cur

    con.execute.side_effect = execute
    assert store.activate(dataset.service_id, "generation", "target") is activated


def test_schema_ddl_postgres_manifest_and_validation(monkeypatch):
    ddl = schema.CLICKHOUSE_FACT_DDL
    assert "ReplacingMergeTree" in ddl and "PARTITION BY (service_id, generation)" in ddl
    assert "ORDER BY (service_id, generation, batch_id, row_ordinal)" in ddl and "TTL" not in ddl
    assert all(f"    {col} " in ddl for col in CLICKHOUSE_FACT_COLUMNS)

    statements = pg_schema_statements()
    for name in ("datasets", "artifacts", "generations", "publications", "service_selection", "schema_state"):
        assert any(f"CREATE TABLE IF NOT EXISTS clickhouse_{name} (" in s for s in statements)

    monkeypatch.setenv("METADATA_DSN", "postgresql://placeholder")
    cl2 = MagicMock()
    schema.maintain_clickhouse_system_tables(cl2)
    assert cl2.execute.call_count == 9

    cl = MagicMock()
    with (
        patch.object(schema, "target_identity", lambda c: "target"),
        patch.object(schema, "_record_schema", MagicMock()),
        patch.object(schema, "maintain_clickhouse_system_tables", MagicMock()),
    ):
        schema.create_clickhouse_schema(cl)
        cl.execute.assert_called_with(schema.CLICKHOUSE_FACT_DDL)

    monkeypatch.delenv("METADATA_DSN", raising=False)
    with pytest.raises(RuntimeError, match="Postgres"):
        schema.create_clickhouse_schema(MagicMock())

    cl3 = MagicMock()
    cl3.execute.return_value = [{"uuid": "target", "engine": "MergeTree"}]
    with pytest.raises(RuntimeError, match="schema"):
        schema.target_identity(cl3)

    tables = [
        {
            "uuid": "target",
            "engine": "ReplacingMergeTree",
            "sorting_key": "service_id, generation, batch_id, row_ordinal",
            "partition_key": "(service_id, generation)",
        }
    ]
    columns = [{"name": name, "type": kind} for name, kind in schema.FACT_TYPES]
    cl3.execute.side_effect = [tables, columns]
    assert schema.target_identity(cl3) == "target"
    cl3.execute.side_effect = [tables, columns[:-1]]
    with pytest.raises(RuntimeError, match="columns mismatch"):
        schema.target_identity(cl3)

    monkeypatch.setenv("METADATA_DSN", "postgresql://placeholder")
    pool, con = MagicMock(), MagicMock()
    pool.connection.return_value.__enter__.return_value = con
    monkeypatch.setattr(schema.pg_connection, "get_pg_pool", lambda: pool)
    schema._record_schema("target")
    con.transaction.assert_called_once()

    monkeypatch.setattr(schema, "get_clickhouse_client", lambda: None)
    with pytest.raises(RuntimeError, match="disabled"):
        schema.main()
    cl4, pool2 = MagicMock(), MagicMock()
    monkeypatch.setattr(schema, "get_clickhouse_client", lambda: cl4)
    monkeypatch.setattr(schema, "create_clickhouse_schema", lambda c: None)
    monkeypatch.setattr(schema.pg_connection, "is_postgres", lambda: True)
    monkeypatch.setattr(schema.pg_connection, "get_pg_pool", lambda: pool2)
    schema.main()
    cl4.close.assert_called_once()
    pool2.close.assert_called_once()


# ── Merged from tests/core/test_high_scale_core.py ──

import contextvars
import os
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from backend.core.high_scale_sections import HighScaleSections, current_clickhouse_scope
from backend.core.query_attribution import Attribution, current_attribution
from backend.high_scale.archive_models import (
    ArchiveArtifact,
    ArchiveManifest,
    ArchiveSourceObject,
    ArchiveState,
)
from backend.high_scale.source_discovery import INITIAL_SOURCE_CURSOR
from psycopg_pool import ConnectionPool

from backend.high_scale.postgres_control import PostgresControlPlane


# ── Merged from tests/core/test_high_scale_postgres_control.py ──
def _fake_store() -> tuple[PostgresControlPlane, MagicMock, MagicMock]:
    pool = MagicMock()
    connection = MagicMock()
    pool.connection.return_value.__enter__.return_value = connection
    connection.transaction.return_value.__enter__.return_value = connection
    return PostgresControlPlane(pool=pool), pool, connection


def _owner_row(
    *,
    service_id: str = "svc",
    epoch: int = 1,
    owner: str = "standard",
    previous: str | None = None,
    cursor: str = "cursor-1",
    drained: bool = False,
    committed: bool = False,
    rollback_allowed: bool = False,
) -> dict[str, object]:
    return {
        "service_id": service_id,
        "owner_epoch": epoch,
        "current_owner": owner,
        "previous_owner": previous,
        "source_cursor": cursor,
        "drain_complete": drained,
        "cutover_committed": committed,
        "rollback_allowed": rollback_allowed,
    }


def _source_row(
    *,
    object_id: str = "object-1",
    service_id: str = "svc",
    object_key: str = "raw/one.gz",
    status: str = "discovered",
    lease_until: datetime | None = None,
    generation: int = 0,
    owner_epoch: int | None = None,
    last_error: str | None = None,
) -> dict[str, object]:
    return {
        "object_id": object_id,
        "service_id": service_id,
        "domain": "request",
        "object_key": object_key,
        "checksum": "sha256:source",
        "size_bytes": 12,
        "version": "v1",
        "status": status,
        "owner": None,
        "lease_until": lease_until,
        "lease_generation": generation,
        "archive_manifest_id": None,
        "owner_epoch": owner_epoch,
        "malformed_rows": 0,
        "accepted_rows": 3,
        "last_error": last_error,
    }


def _archive_row(
    *,
    manifest_id: str = "manifest-1",
    service_id: str = "svc",
    state: str = "manifest_prepared",
    owner_epoch: int = 1,
) -> dict[str, object]:
    start = datetime(2026, 9, 1, tzinfo=UTC)
    return {
        "manifest_id": manifest_id,
        "service_id": service_id,
        "domain": "request",
        "source_key": "raw/one.gz",
        "source_checksum": "sha256:source",
        "source_size": 12,
        "source_version": "v1",
        "artifact_uri": "s3://archive/manifest-1.parquet",
        "artifact_checksum": "sha256:artifact",
        "artifact_size": 128,
        "row_count": 3,
        "byte_count": 256,
        "canonical_digest": "digest",
        "schema_version": "archive.v1",
        "transform_version": "normalize.v1",
        "coverage_start": start,
        "coverage_end": start + timedelta(minutes=1),
        "retention_deadline": start + timedelta(days=1),
        "deletion_authorization_deadline": start + timedelta(days=2),
        "archive_epoch": 1,
        "owner_epoch": owner_epoch,
        "state": state,
    }


def test_schema_is_explicit_and_transactional() -> None:
    store, pool, connection = _fake_store()

    store.create_schema()

    pool.connection.assert_called_once_with()
    connection.transaction.assert_called_once_with()
    ddl = connection.execute.call_args.args[0]
    assert "CREATE TABLE IF NOT EXISTS high_scale_ownership" in ddl
    assert "CREATE TABLE IF NOT EXISTS high_scale_source_objects" in ddl
    assert "CREATE TABLE IF NOT EXISTS high_scale_archive_manifests" in ddl
    assert "CREATE TABLE IF NOT EXISTS high_scale_batch_claims" in ddl
    assert "CREATE TABLE IF NOT EXISTS high_scale_deletion_authorizations" in ddl


def test_operational_snapshot_aggregates_source_and_publication_state() -> None:
    store, _, connection = _fake_store()
    source_counts = MagicMock(fetchall=MagicMock(return_value=[("request", "claimed", 4)]))
    source_age = MagicMock(fetchall=MagicMock(return_value=[("request", 9.5)]))
    publications = MagicMock(fetchall=MagicMock(return_value=[("request", 2, 120, 3.25)]))
    connection.execute.side_effect = [MagicMock(), source_counts, source_age, publications]

    snapshot = store.operational_snapshot("svc")

    assert snapshot == {
        "source_objects": [{"domain": "request", "status": "claimed", "count": 4}],
        "source_age": [{"domain": "request", "age_seconds": 9.5}],
        "publications": [
            {
                "domain": "request",
                "pending": 2,
                "published_rows": 120,
                "lag_seconds": 3.25,
            }
        ],
    }
    assert connection.execute.call_count == 4


def test_discover_source_tolerates_a_sibling_domain_discovering_the_same_object() -> None:
    """Regression test: rum_vitals and rum_errors both list the FOS prefix
    raw/rum/, so the same object is legitimately discovered by both
    domains' pages — whichever wins the race keeps the row. Rejecting the
    second domain's discovery as a "changed identity" made every subsequent
    tick of the losing domain's page raise and abort, permanently starving
    it (observed live during RUM qualification). Only a genuine
    checksum/version change is a real identity conflict."""
    store, _, connection = _fake_store()
    owner = _owner_row(owner="high_scale", epoch=1)
    existing = _source_row(status="discovered")  # discovered under domain="request" per _source_row's default
    cursors = [MagicMock(), MagicMock()]
    cursors[0].fetchone.return_value = owner
    cursors[1].fetchone.return_value = existing
    connection.execute.side_effect = cursors

    result = store.discover_source(
        "svc",
        "rum_errors",
        "raw/one.gz",
        "sha256:source",  # matches _source_row's checksum
        size_bytes=12,
        version="v1",  # matches _source_row's version
        expected_owner="high_scale",
        expected_owner_epoch=1,
    )

    assert result.object_id == "object-1"
    # Only the two locking SELECTs ran — no INSERT attempted, no error.
    assert len(connection.execute.call_args_list) == 2


def test_discover_source_rejects_a_real_checksum_change() -> None:
    store, _, connection = _fake_store()
    owner = _owner_row(owner="high_scale", epoch=1)
    existing = _source_row(status="discovered")
    cursors = [MagicMock(), MagicMock()]
    cursors[0].fetchone.return_value = owner
    cursors[1].fetchone.return_value = existing
    connection.execute.side_effect = cursors

    with pytest.raises(ValueError, match="identity changed"):
        store.discover_source(
            "svc",
            "request",
            "raw/one.gz",
            "sha256:different",
            size_bytes=12,
            version="v1",
            expected_owner="high_scale",
            expected_owner_epoch=1,
        )


def test_claim_returns_existing_lease_without_stealing_it() -> None:
    store, _, connection = _fake_store()
    now = datetime(2026, 9, 11, tzinfo=UTC)
    owner = _owner_row()
    source = _source_row(lease_until=now + timedelta(seconds=30), generation=4)
    cursors = [MagicMock(), MagicMock()]
    cursors[0].fetchone.return_value = owner
    cursors[1].fetchone.return_value = source
    connection.execute.side_effect = cursors

    result = store.claim_source(
        "svc",
        "raw/one.gz",
        "worker-2",
        expected_owner="standard",
        expected_owner_epoch=1,
        now=now,
    )

    assert result.object_id == "object-1"
    assert result.claimed is False
    assert result.lease_generation == 4
    assert len(connection.execute.call_args_list) == 2


def test_claim_is_idempotent_for_the_current_worker() -> None:
    store, _, connection = _fake_store()
    now = datetime(2026, 9, 11, tzinfo=UTC)
    owner = _owner_row()
    source = _source_row(
        lease_until=now + timedelta(seconds=30),
        generation=4,
    )
    source["owner"] = "worker-1"
    cursors = [MagicMock(), MagicMock()]
    cursors[0].fetchone.return_value = owner
    cursors[1].fetchone.return_value = source
    connection.execute.side_effect = cursors

    result = store.claim_source(
        "svc",
        "raw/one.gz",
        "worker-1",
        expected_owner="standard",
        expected_owner_epoch=1,
        now=now,
    )

    assert result.object_id == "object-1"
    assert result.claimed is True
    assert result.lease_generation == 4
    assert len(connection.execute.call_args_list) == 2


def test_claim_fences_stale_owner_epoch_before_source_update() -> None:
    store, _, connection = _fake_store()
    connection.execute.return_value.fetchone.return_value = _owner_row(epoch=2)

    with pytest.raises(ValueError, match="owner epoch"):
        store.claim_source(
            "svc",
            "raw/one.gz",
            "worker-1",
            expected_owner="standard",
            expected_owner_epoch=1,
        )

    assert len(connection.execute.call_args_list) == 1


def test_mark_source_missing_stamps_terminal_status_and_error() -> None:
    store, _, connection = _fake_store()
    owner = _owner_row(owner="high_scale", epoch=1)
    source = _source_row(status="claimed", owner_epoch=1)
    cursors = [MagicMock(), MagicMock(), MagicMock()]
    cursors[0].fetchone.return_value = owner
    cursors[1].fetchone.return_value = source
    connection.execute.side_effect = cursors

    store.mark_source_missing(
        "svc",
        "raw/one.gz",
        expected_owner="high_scale",
        expected_owner_epoch=1,
        error="source object is missing from storage: raw/one.gz",
    )

    update_sql, update_params = connection.execute.call_args_list[2].args
    assert "status='source_missing'" in update_sql
    assert update_params[0] == "source object is missing from storage: raw/one.gz"
    assert update_params[-1] == source["object_id"]


def test_mark_source_missing_is_a_no_op_when_already_missing() -> None:
    store, _, connection = _fake_store()
    owner = _owner_row(owner="high_scale", epoch=1)
    source = _source_row(status="source_missing", owner_epoch=1)
    cursors = [MagicMock(), MagicMock()]
    cursors[0].fetchone.return_value = owner
    cursors[1].fetchone.return_value = source
    connection.execute.side_effect = cursors

    store.mark_source_missing(
        "svc",
        "raw/one.gz",
        expected_owner="high_scale",
        expected_owner_epoch=1,
        error="retry",
    )

    # Only the two locking SELECTs — no UPDATE issued for an already-terminal row.
    assert len(connection.execute.call_args_list) == 2


def test_mark_source_missing_refuses_to_override_acknowledged_source() -> None:
    store, _, connection = _fake_store()
    owner = _owner_row(owner="high_scale", epoch=1)
    source = _source_row(status="acknowledged", owner_epoch=1)
    cursors = [MagicMock(), MagicMock()]
    cursors[0].fetchone.return_value = owner
    cursors[1].fetchone.return_value = source
    connection.execute.side_effect = cursors

    with pytest.raises(ValueError, match="durably archived"):
        store.mark_source_missing(
            "svc",
            "raw/one.gz",
            expected_owner="high_scale",
            expected_owner_epoch=1,
            error="source object is missing from storage: raw/one.gz",
        )


def test_mark_source_missing_fences_stale_owner_epoch() -> None:
    store, _, connection = _fake_store()
    connection.execute.return_value.fetchone.return_value = _owner_row(owner="high_scale", epoch=2)

    with pytest.raises(ValueError, match="owner epoch"):
        store.mark_source_missing(
            "svc",
            "raw/one.gz",
            expected_owner="high_scale",
            expected_owner_epoch=1,
            error="source object is missing from storage: raw/one.gz",
        )

    assert len(connection.execute.call_args_list) == 1


def test_cursor_advancement_is_owner_epoch_fenced() -> None:
    store, _, connection = _fake_store()
    current = _owner_row(owner="high_scale", epoch=4, cursor="cursor-1")
    updated = _owner_row(owner="high_scale", epoch=4, cursor="cursor-2")
    cursors = [MagicMock(), MagicMock(), MagicMock()]
    cursors[0].fetchone.return_value = current
    cursors[2].fetchone.return_value = updated
    connection.execute.side_effect = cursors

    result = store.advance_source_cursor(
        "svc",
        "cursor-2",
        expected_owner="high_scale",
        expected_owner_epoch=4,
    )

    assert result.source_cursor == "cursor-2"
    assert connection.execute.call_args_list[1].args[1] == (
        "cursor-2",
        "svc",
        4,
        "high_scale",
    )


def test_source_cursor_for_never_falls_back_to_owner_level_cursor() -> None:
    """Regression test: different domains list different FOS prefixes
    (request vs. rum). Falling back to the shared owner-level cursor for a
    domain that has never advanced can hand it a StartAfter key entirely
    outside its own Prefix — observed live as rum_vitals/rum_errors
    permanently stuck at zero discovered objects, silently, because the
    inherited request-domain cursor made every listing return empty. A
    domain with no cursor of its own must get its own initial cursor."""
    store, _, connection = _fake_store()
    cursor = MagicMock()
    cursor.fetchone.return_value = None
    connection.execute.return_value = cursor

    result = store.source_cursor_for("svc", "rum_vitals")

    assert result == INITIAL_SOURCE_CURSOR
    # Only the per-domain lookup ran (plus the read-only transaction set-up)
    # — no owner-row fallback query.
    executed_sql = [c.args[0] for c in connection.execute.call_args_list]
    assert not any("high_scale_ownership" in sql for sql in executed_sql)


def test_source_cursor_for_returns_the_domains_own_persisted_cursor() -> None:
    store, _, connection = _fake_store()
    cursor = MagicMock()
    cursor.fetchone.return_value = ("__terminal__:raw/rum/a.gz",)
    connection.execute.return_value = cursor

    assert store.source_cursor_for("svc", "rum_vitals") == "__terminal__:raw/rum/a.gz"


def test_database_errors_propagate_from_transaction() -> None:
    store, _, connection = _fake_store()
    connection.execute.side_effect = RuntimeError("database unavailable")

    with pytest.raises(RuntimeError, match="database unavailable"):
        store.create_schema()


def test_archive_transition_requires_owner_epoch_and_adjacent_state() -> None:
    store, _, connection = _fake_store()
    current = _archive_row()
    updated = _archive_row(state="manifest_committed")
    cursors = [MagicMock(), MagicMock(), MagicMock()]
    cursors[0].fetchone.return_value = current
    cursors[2].fetchone.return_value = updated
    connection.execute.side_effect = cursors

    result = store.transition_archive_manifest(
        "manifest-1",
        expected_state=ArchiveState.MANIFEST_PREPARED,
        next_state=ArchiveState.MANIFEST_COMMITTED,
        expected_owner_epoch=1,
    )

    assert result.state is ArchiveState.MANIFEST_COMMITTED
    assert connection.execute.call_args_list[1].args[1] == (
        "manifest_committed",
        "manifest-1",
        1,
        "manifest_prepared",
    )

    connection.execute.reset_mock()
    stale_cursor = MagicMock()
    stale_cursor.fetchone.return_value = current
    connection.execute.side_effect = [stale_cursor]
    with pytest.raises(ValueError, match="owner epoch"):
        store.transition_archive_manifest(
            "manifest-1",
            expected_state=ArchiveState.MANIFEST_PREPARED,
            next_state=ArchiveState.MANIFEST_COMMITTED,
            expected_owner_epoch=2,
        )


def test_deletion_authorization_rejects_active_replay_lease_without_database_call() -> None:
    store, _, connection = _fake_store()

    with pytest.raises(ValueError, match="active replay lease"):
        store.authorize_source_delete(
            "svc",
            "raw/one.gz",
            manifest_id="manifest-1",
            expected_owner_epoch=1,
            replay_lease_active=True,
        )

    connection.execute.assert_not_called()


def test_deletable_sources_queries_committed_and_eligible_manifests_past_deadline() -> None:
    store, _, connection = _fake_store()
    cursor = MagicMock()
    cursor.fetchall.return_value = [_source_row(status="acknowledged"), _source_row(status="archived")]
    connection.execute.return_value = cursor

    result = store.deletable_sources("svc", limit=50)

    assert len(result) == 2
    assert all(r.object_id == "object-1" for r in result)
    sql, params = connection.execute.call_args.args
    assert "high_scale_source_objects" in sql
    assert "high_scale_archive_manifests" in sql
    assert "'archived', 'acknowledged'" in sql
    assert "'manifest_committed', 'deletion_eligible'" in sql
    assert params == ("svc", 50)


def test_deletable_sources_rejects_non_positive_limit() -> None:
    store, _, _ = _fake_store()
    with pytest.raises(ValueError, match="limit"):
        store.deletable_sources("svc", limit=0)


def test_manifests_covering_queries_by_service_domain_and_overlap() -> None:
    store, _, connection = _fake_store()
    cursor = MagicMock()
    cursor.fetchall.return_value = [_archive_row(manifest_id="m1"), _archive_row(manifest_id="m2")]
    connection.execute.return_value = cursor

    start = datetime(2026, 9, 1, tzinfo=UTC)
    end = datetime(2026, 9, 1, 1, tzinfo=UTC)
    result = store.manifests_covering("svc", "request", start, end)

    assert [m.manifest_id for m in result] == ["m1", "m2"]
    sql, params = connection.execute.call_args.args
    assert "high_scale_archive_manifests" in sql
    assert "coverage_end > %s" in sql
    assert "coverage_start < %s" in sql
    assert params == ("svc", "request", start, end)


def test_manifests_covering_rejects_inverted_range() -> None:
    store, _, _ = _fake_store()
    start = datetime(2026, 9, 1, tzinfo=UTC)
    with pytest.raises(ValueError, match="range"):
        store.manifests_covering("svc", "request", start, start - timedelta(minutes=1))


def _manifest(*, service_id: str = "svc", manifest_id: str = "manifest-1") -> ArchiveManifest:
    start = datetime(2026, 9, 1, tzinfo=UTC)
    source = ArchiveSourceObject(service_id, "request", "raw/one.gz", "sha256:source", 12, "v1")
    artifact = ArchiveArtifact(
        "s3://archive/manifest-1.parquet",
        "sha256:artifact",
        128,
        3,
        256,
        "digest",
        "archive.v1",
        "normalize.v1",
    )
    return ArchiveManifest(
        manifest_id,
        source,
        artifact,
        start,
        start + timedelta(minutes=1),
        start + timedelta(days=1),
        start + timedelta(days=2),
        1,
    )


@pytest.mark.postgres_integration
def test_postgres_control_plane_end_to_end() -> None:
    dsn = os.getenv("HIGH_SCALE_POSTGRES_TEST_DSN")
    if not dsn:
        pytest.skip("HIGH_SCALE_POSTGRES_TEST_DSN is not set")

    service_id = f"test-high-scale-{uuid4()}"
    pool = ConnectionPool(
        conninfo=dsn,
        kwargs={"autocommit": False},
        min_size=1,
        max_size=2,
        timeout=5,
    )
    store = PostgresControlPlane(pool=pool, pool_timeout=5)
    manifest = _manifest(service_id=service_id)
    try:
        pool.open(wait=True)
        store.create_schema()
        owner = store.initialize_owner(service_id, owner="standard", source_cursor="cursor-0")
        source = store.discover_source(
            service_id,
            "request",
            "raw/one.gz",
            "sha256:source",
            size_bytes=12,
            version="v1",
            expected_owner="standard",
            expected_owner_epoch=owner.owner_epoch,
        )
        store.claim_source(
            service_id,
            source.object_key,
            "worker-1",
            expected_owner="standard",
            expected_owner_epoch=owner.owner_epoch,
        )
        store.register_archive_manifest(manifest, owner_epoch=owner.owner_epoch)
        for current, next_state in (
            (ArchiveState.ARTIFACT_UPLOADING, ArchiveState.ARTIFACT_VERIFIED),
            (ArchiveState.ARTIFACT_VERIFIED, ArchiveState.MANIFEST_PREPARED),
            (ArchiveState.MANIFEST_PREPARED, ArchiveState.MANIFEST_COMMITTED),
        ):
            store.transition_archive_manifest(
                manifest.manifest_id,
                expected_state=current,
                next_state=next_state,
                expected_owner_epoch=owner.owner_epoch,
            )
        store.acknowledge_sources_batch(
            service_id,
            [source.object_key],
            manifest_id=manifest.manifest_id,
            expected_owner_epoch=owner.owner_epoch,
        )
        store.transition_archive_manifest(
            manifest.manifest_id,
            expected_state=ArchiveState.MANIFEST_COMMITTED,
            next_state=ArchiveState.DELETION_ELIGIBLE,
            expected_owner_epoch=owner.owner_epoch,
        )
        store.authorize_source_delete(
            service_id,
            source.object_key,
            manifest_id=manifest.manifest_id,
            expected_owner_epoch=owner.owner_epoch,
            now=manifest.deletion_authorization_deadline + timedelta(seconds=1),
        )
        store.mark_source_deleted(
            service_id,
            source.object_key,
            manifest_id=manifest.manifest_id,
            expected_owner_epoch=owner.owner_epoch,
        )
        assert store.archive_manifest(manifest.manifest_id).state is ArchiveState.SOURCE_DELETED
    finally:
        try:
            with pool.connection(timeout=5) as connection, connection.transaction():
                connection.execute(
                    "DELETE FROM high_scale_deletion_authorizations WHERE object_id IN "
                    "(SELECT object_id FROM high_scale_source_objects WHERE service_id=%s)",
                    (service_id,),
                )
                connection.execute(
                    "DELETE FROM high_scale_source_objects WHERE service_id=%s",
                    (service_id,),
                )
                connection.execute(
                    "DELETE FROM high_scale_archive_manifests WHERE service_id=%s",
                    (service_id,),
                )
                connection.execute(
                    "DELETE FROM high_scale_ownership WHERE service_id=%s",
                    (service_id,),
                )
        finally:
            pool.close()


# ── Merged from tests/core/test_high_scale_sections.py ──
def test_sections_propagate_service_and_query_ownership_to_workers() -> None:
    trace = HighScaleSections("service-test", "security")
    token = current_attribution.set(Attribution(kind="admin", request_id="request-test"))
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [
                pool.submit(contextvars.copy_context().run, trace.run, name, current_clickhouse_scope, source="fact")
                for name in ("tls", "bots")
            ]
            rows = [future.result() for future in futures]
    finally:
        current_attribution.reset(token)
    assert {row["service_id"] for row in rows} == {"service-test"}
    assert {row["request_id"] for row in rows} == {"request-test"}
    assert {row["section"] for row in rows} == {"security.tls", "security.bots"}
    assert len({row["section_id"] for row in rows}) == 2
    assert len(trace.timings()) == 2
    assert all(row["source"] == "fact" and row["current_execution"] is True for row in trace.timings())
    assert current_clickhouse_scope()["service_id"] is None


def test_named_task_queue_wait_is_separate_from_section_execution(monkeypatch) -> None:
    import threading

    clock = [0.0]
    monkeypatch.setattr("backend.core.high_scale_sections.time.perf_counter", lambda: clock[0])
    trace = HighScaleSections("service-test", "network")
    ready, release = threading.Event(), threading.Event()

    def occupy():
        ready.set()
        assert release.wait(3)

    def execute():
        clock[0] += 0.05
        return "result"

    with ThreadPoolExecutor(max_workers=1) as pool:
        blocker = pool.submit(occupy)
        assert ready.wait(3)
        future = trace.submit(pool, "heatmap", execute, source="fact")
        clock[0] = 0.02
        release.set()
        assert future.result(timeout=3) == "result"
        blocker.result(timeout=3)
    observation = trace.timings()[0]
    assert observation["executor_dispatch"] == "thread_pool"
    assert observation["executor_wait_ms"] == pytest.approx(20)
    assert observation["time_ms"] == pytest.approx(50)


def test_persistence_never_falls_back_to_another_active_service(monkeypatch) -> None:
    from unittest.mock import MagicMock

    from backend import config
    from backend.core.metadata import usage_log
    from backend.utils import telemetry

    active = MagicMock(return_value="wrong-active-service")
    persist = MagicMock()
    monkeypatch.setattr(config, "get_active_service_id", active)
    monkeypatch.setattr(usage_log, "log_telemetry_query", persist)
    token = telemetry._PAGE_LOAD_ID.set("page-test")
    try:
        telemetry.record_telemetry_query("clickhouse", "SELECT 1", 2, allow_active_service_fallback=False)
        assert active.call_count == persist.call_count == 0
        telemetry.record_telemetry_query(
            "clickhouse",
            "SELECT 1",
            2,
            service_id="service-test",
            request_id="request-test",
            allow_active_service_fallback=False,
        )
    finally:
        telemetry._PAGE_LOAD_ID.reset(token)
    assert active.call_count == 0
    assert persist.call_args.kwargs["service_id"] == "service-test"
    assert persist.call_args.kwargs["request_id"] == "request-test"


def test_nested_sections_reset_after_errors_and_remain_separate() -> None:
    parent = HighScaleSections("service-parent", "network")
    child = HighScaleSections("service-child", "cmcd")
    scopes = []

    def fails():
        scopes.append(current_clickhouse_scope())
        raise ValueError("private-value")

    def outer():
        scopes.append(current_clickhouse_scope())
        with pytest.raises(ValueError, match="private-value"):
            child.run("overview", fails, source="projected_fact")
        scopes.append(current_clickhouse_scope())

    parent.run("health", outer, source="fact")
    assert [row["service_id"] for row in scopes] == ["service-parent", "service-child", "service-parent"]
    assert scopes[1]["parent_section_id"] == scopes[0]["section_id"]
    assert child.timings()[0]["outcome"] == "error"
    assert "private-value" not in str(child.timings())
    assert current_clickhouse_scope()["section_id"] is None
