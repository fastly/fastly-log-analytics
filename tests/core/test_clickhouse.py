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
from uuid import UUID, uuid4

import duckdb
import httpx
import pytest
from backend.core.clickhouse_schema import CLICKHOUSE_SCHEMA_VERSION, create_clickhouse_schema, target_identity
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
    get_clickhouse_client,
)
from backend.core.clickhouse_manifest import Artifact, Dataset, PgManifest
from backend.core.clickhouse_publication import (
    PAYLOAD_COLUMNS,
    DurableBatch,
    FosArtifacts,
    activate_generation,
    build_batch_id,
    canonical_bytes,
    canonical_row,
    digest_rows,
    full_rebuild,
    publish_batch,
    readiness,
    replay_unpublished,
    verify_generation,
)
from backend.core.clickhouse_publication import build_batch_id as _rows_build_batch_id
from backend.core.metadata.pg_schema import pg_schema_statements
from backend.models.common import DebugQuery
from backend.utils import telemetry
from scripts import clickhouse_replay
from scripts.clickhouse_replay import parser, run


# ── Merged from tests/core/test_clickhouse_publication.py ──
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


def test_canonical_preserves_null_empty_and_duplicate_ordinals():
    assert digest_rows((row(),)) != digest_rows((row(), row(1)))
    assert canonical_bytes(row()) != canonical_bytes((*row()[:3], None, *row()[4:]))
    assert (
        canonical_row(
            dict(
                zip(
                    ("source_identity", "row_ordinal", "timestamp", "country", "ip", "url", "conn_requests"),
                    row(),
                    strict=True,
                )
            )
        )
        == row()
    )


def test_deterministic_identity_is_tenant_and_dataset_scoped():
    assert build_batch_id("a", "d", "c") == build_batch_id("a", "d", "c")
    assert build_batch_id("a", "d", "c") != build_batch_id("b", "d", "c")
    assert build_batch_id("a", "d", "c") != build_batch_id("a", "e", "c")


def test_production_manifest_rejects_sqlite(monkeypatch):
    monkeypatch.delenv("METADATA_DSN", raising=False)
    with pytest.raises(RuntimeError, match="Postgres"):
        PgManifest()


def test_bad_payload_never_claims_or_inserts():
    dataset, artifact, batch = fixture_manifest()
    store = MagicMock()
    store.artifact.return_value = artifact
    client = MagicMock()
    with pytest.raises(ValueError, match="payload"):
        publish_batch(dataset.service_id, replace(batch, rows=(row(),)), generation="g", store=store, client=client)
    store.claim.assert_not_called()
    client.insert_rows.assert_not_called()


def test_conflicting_payload_never_claims_even_with_same_count():
    dataset, artifact, batch = fixture_manifest()
    store = MagicMock()
    store.artifact.return_value = artifact
    client = MagicMock()
    with pytest.raises(ValueError, match="payload"):
        publish_batch(
            dataset.service_id, replace(batch, rows=(row(), row(3))), generation="g", store=store, client=client
        )
    store.claim.assert_not_called()


def test_insert_failure_records_sanitized_failure_and_raises(monkeypatch):
    dataset, artifact, batch = fixture_manifest()
    store = MagicMock()
    store.artifact.return_value = artifact
    store.claim.return_value = 1
    store.generation.return_value = {"target_identity": "target"}
    monkeypatch.setattr("backend.core.clickhouse_publication.target_identity", lambda c: "target")
    client = MagicMock()
    client.insert_rows.side_effect = RuntimeError("credentials must not escape")
    with pytest.raises(RuntimeError, match="publication failed: RuntimeError"):
        publish_batch(dataset.service_id, batch, generation="g", store=store, client=client)
    assert store.finish.call_args.kwargs["error"] == "RuntimeError"
    assert store.finish.call_args.kwargs["digest"] is None


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
        assert "log_facts FINAL" in sql
        assert params["service"] == dataset.service_id
        if "SELECT count()" in sql:
            return [{"n": len(batch.rows)}]
        return [dict(zip(PAYLOAD_COLUMNS, row, strict=True)) for row in batch.rows]

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
    if outcome == "failed":
        with pytest.raises(ValueError, match="manifest unavailable"):
            publish_batch(dataset.service_id, batch, generation="g", store=store, client=client)
        store.claim.assert_not_called()
    else:
        assert publish_batch(dataset.service_id, batch, generation="g", store=store, client=client).status == outcome
    record.assert_called_once_with(outcome)


def test_publication_telemetry_and_failure_recording_never_mask_result(published_fixture, monkeypatch):
    dataset, _, batch, store, client = published_fixture
    monkeypatch.setattr(
        "backend.core.clickhouse_publication.record_publication", MagicMock(side_effect=RuntimeError("telemetry"))
    )
    monkeypatch.setattr("backend.core.clickhouse_publication.logger.info", MagicMock(side_effect=RuntimeError("log")))
    assert publish_batch(dataset.service_id, batch, generation="g", store=store, client=client).status == "published"
    client.insert_rows.side_effect = ValueError("original")
    store.finish.side_effect = RuntimeError("manifest unavailable")
    with pytest.raises(RuntimeError, match="publication failed: ValueError"):
        publish_batch(dataset.service_id, batch, generation="g", store=store, client=client)


def test_generation_verification_rejects_foreign_membership_and_digest(published_fixture):
    dataset, artifact, _, store, client = published_fixture
    assert verify_generation(dataset.service_id, "g", store=store, client=client) == dataset
    store.artifacts.return_value = [replace(artifact, ordinal_start=1)]
    with pytest.raises(ValueError, match="coverage"):
        verify_generation(dataset.service_id, "g", store=store, client=client)
    store.artifacts.return_value = [artifact]
    store.dataset.return_value = replace(dataset, canonical_digest="wrong")
    with pytest.raises(ValueError, match="digest"):
        verify_generation(dataset.service_id, "g", store=store, client=client)


def test_full_rebuild_and_resume_keep_distinct_generation(published_fixture):
    dataset, artifact, batch, store, client = published_fixture
    store.begin_generation.return_value = "new-generation"
    store.pending.side_effect = [[artifact.batch_id], []]
    result = full_rebuild(
        dataset.service_id, dataset.dataset_id, loader=lambda artifact: batch, store=store, client=client
    )
    assert result.generation == "new-generation"
    assert result.activated and result.published == 1
    store.begin_generation.assert_called_once_with(dataset.service_id, dataset.dataset_id, "target")
    store.pending.side_effect = [[artifact.batch_id], ["later"]]
    result = replay_unpublished(
        dataset.service_id, generation="g", loader=lambda artifact: batch, store=store, client=client
    )
    assert not result.activated
    with pytest.raises(ValueError, match="limit"):
        full_rebuild(
            dataset.service_id, dataset.dataset_id, loader=lambda artifact: batch, limit=0, store=store, client=client
        )


# ── Merged from tests/core/test_clickhouse_client.py ──
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
        client = ch.ClickHouseClient(settings, transport=httpx.MockTransport(handler))
        opened.append(client)
        return client

    yield build
    for client in opened:
        client.close()


def response(rows=None):
    return httpx.Response(
        200,
        json={
            "data": rows if rows is not None else [{"ok": 1}],
            "statistics": {"rows_read": 7, "bytes_read": 81},
        },
    )


def test_origin_projection_tables_and_columns_are_allowlisted():
    for table in ("origin_minute_summary", "origin_minute_dimensions"):
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
    ):
        assert column in ch.CLICKHOUSE_HIGH_SCALE_COLUMNS


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
        assert form["param_country"] == ["x' OR 1=1 --\\n\\t\\\\N"]
        assert form["param_limit"] == ["2"]
        assert "param_country" not in request.url.params
        assert value not in str(request.url)
        assert request.url.params["default_format"] == "JSON"
        assert float(request.url.params["max_execution_time"]) > 0
        assert request.url.params["wait_end_of_query"] == "1"
        assert request.url.params["max_memory_usage"] == "1073741824"
        assert request.url.params["max_threads"] == "2"
        assert request.url.params["max_bytes_before_external_group_by"] == "134217728"
        assert request.url.params["max_bytes_before_external_sort"] == "134217728"
        UUID(request.url.params["query_id"])
        return response([{"country": value, "n": 2}])

    client = clients(handler)
    assert client.execute(sql, {"country": value, "limit": 2}) == [{"country": value, "n": 2}]
    client.execute(sql, {"country": value, "limit": 2})
    assert seen[0].url.params["query_id"] != seen[1].url.params["query_id"]


def test_internal_ddl_accepts_empty_response(clients):
    sql = f"CREATE TABLE IF NOT EXISTS {ch.CLICKHOUSE_FACT_TABLE} (service_id String) ENGINE = Memory"

    def handler(request):
        assert query_form(request)["query"] == [sql]
        return httpx.Response(200, content=b"")

    assert clients(handler).execute(sql) == []


def test_insert_internal_columns_and_values_are_separate(clients, settings):
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
    row = (
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
            f"INSERT INTO `{ch.CLICKHOUSE_FACT_TABLE}` ({', '.join(f'`{c}`' for c in columns)}) "
            "FORMAT JSONCompactEachRow"
        )
        assert request.url.params["async_insert"] == "0"
        assert request.url.params["wait_end_of_query"] == "1"
        assert float(request.url.params["max_execution_time"]) == settings.insert_timeout_s
        assert request.url.params["max_memory_usage"] == "536870912"
        assert request.url.params["max_threads"] == "2"
        assert request.url.params["max_insert_threads"] == "1"
        assert request.url.params["max_block_size"] == "2048"
        body = json.loads(request.content)
        assert body[:4] == list(row[:4])
        assert body[4] == "2026-09-07 00:00:00.000000"
        assert body[5:] == list(row[5:])
        return httpx.Response(200)

    assert clients(handler).insert_rows(ch.CLICKHOUSE_FACT_TABLE, columns, [row]) is None


@pytest.mark.parametrize("table", ["arbitrary_table", "system.users", "logs; DROP TABLE logs", "`logs`", ""])
def test_insert_rejects_non_allowlisted_tables(clients, table):
    client = clients(lambda _: pytest.fail("HTTP called for invalid table"))
    with pytest.raises(ValueError, match="table"):
        client.insert_rows(table, ["country"], [("US",)])


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
    client = clients(lambda _: pytest.fail("HTTP called for invalid insert"))
    with pytest.raises(ValueError):
        client.insert_rows(ch.CLICKHOUSE_FACT_TABLE, columns, rows)


def test_empty_insert_is_noop(clients):
    client = clients(lambda _: pytest.fail("HTTP called for empty batch"))
    client.insert_rows(ch.CLICKHOUSE_FACT_TABLE, ["country"], [])


def test_delete_batch_rows_binds_uuid_parameter_type(clients):
    seen = {}

    def handler(request):
        seen.update(query_form(request))
        return httpx.Response(200, content=b"")

    clients(handler).delete_batch_rows("request_facts", "123e4567-e89b-12d3-a456-426614174000")

    assert seen["query"] == [
        "ALTER TABLE `request_facts` DELETE WHERE batch_id={batch_id:UUID} SETTINGS mutations_sync=2"
    ]
    assert seen["param_batch_id"] == ["123e4567-e89b-12d3-a456-426614174000"]


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
    assert secret not in "".join(traceback.format_exception(error.value))
    assert secret not in caplog.text
    assert error.value.query_id
    if failure == "server":
        assert error.value.status_code == 503


def test_insert_timeout_not_retried(clients):
    calls = []

    def handler(request):
        calls.append(request)
        raise httpx.ReadTimeout("ambiguous write", request=request)

    with pytest.raises(ch.ClickHouseError, match="timeout"):
        clients(handler).insert_rows(ch.CLICKHOUSE_FACT_TABLE, ["country"], [("US",)])
    assert len(calls) == 1


def test_health_probes_query_path_and_does_not_fallback(clients):
    client = clients(lambda _: response())
    assert client.health()["ok"] is True
    with pytest.raises(ch.ClickHouseError):
        clients(lambda _: httpx.Response(503)).health()


def test_storage_health_is_authenticated_internal_default_disk_query(clients):
    def handler(request):
        sql = query_form(request)["query"][0]
        assert "system.disks" in sql and "name = 'default'" in sql
        assert request.headers["authorization"].startswith("Basic ")
        return response([{"free_space": 40, "total_space": 100}])

    assert clients(handler).storage_health() == {"up": 1, "disk_free": 40, "disk_total": 100}
    with pytest.raises(ch.ClickHouseError):
        clients(lambda _: httpx.Response(503)).storage_health()
    with pytest.raises(ValueError):
        clients(lambda _: response([])).storage_health()


def test_active_query_monitor_is_non_cancellable_and_cleaned(clients):
    from backend.core.query_registry import query_registry

    active = []

    def handler(_):
        entries = [q for q in query_registry._queries.values() if q.db_type == "ClickHouse"]
        assert len(entries) == 1
        entry = entries[0]
        assert entry._con_id is None and entry._con_ref is None
        active.append(entry.query_id)
        assert "PRIVATE" not in entry.sql
        return response()

    clients(handler).execute("SELECT {secret:String}", {"secret": "PRIVATE"})
    assert active
    assert active[0] not in query_registry._queries


def test_pool_acquisition_is_bounded_and_released(clients):
    entered = threading.Event()
    release = threading.Event()

    def handler(_):
        entered.set()
        assert release.wait(3)
        return response()

    client = clients(handler)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(client.execute, "SELECT 1")
        try:
            assert entered.wait(3)
            with pytest.raises(ch.ClickHouseError, match="pool"):
                client.execute("SELECT 1")
        finally:
            release.set()
        assert future.result() == [{"ok": 1}]
    assert client.execute("SELECT 1") == [{"ok": 1}]


def test_close_is_idempotent_and_rejects_reuse(clients):
    client = clients(lambda _: response())
    client.close()
    client.close()
    with pytest.raises(ch.ClickHouseError, match="closed"):
        client.execute("SELECT 1")


def test_statistics_are_recorded_without_values(clients, monkeypatch):
    events = []
    monkeypatch.setattr(ch.logger, "info", lambda event, **kw: events.append((event, kw)))
    telemetry.start_call_tracking()
    assert clients(lambda _: response()).execute("SELECT {secret:String}", {"secret": "PRIVATE"}) == [{"ok": 1}]
    event, stats = events[-1]
    assert event == "clickhouse.operation"
    assert stats["rows_read"] == 7
    assert stats["bytes_read"] == 81
    assert stats["duration_ms"] >= 0
    assert stats["query_id"]
    assert "PRIVATE" not in str(events)
    assert telemetry.get_tracked_calls()[-1]["service"] == "ClickHouse"


def test_client_phases_and_section_identity_are_measured_at_existing_seams(clients, monkeypatch):
    from backend.core.high_scale_sections import HighScaleSections

    from backend.core.query_attribution import Attribution, current_attribution

    clock = [0.0]
    captured = []
    registered = []
    transmitted = []
    monkeypatch.setattr(ch.time, "perf_counter", lambda: clock[0])
    monkeypatch.setattr(ch, "_record_operation", lambda **stats: captured.append(stats))
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
        result = response()
        decode = result.json

        def json_body():
            clock[0] += 0.03
            payload = decode()
            payload["statistics"]["elapsed"] = 0.004
            return payload

        result.json = json_body
        return result

    client = clients(handler)
    client._slots = Slots()
    token = current_attribution.set(Attribution(kind="admin", request_id="request-test"))
    try:
        sections = HighScaleSections("service-test", "dashboard")
        result = sections.run("summary", lambda: client.execute("SELECT 1"), source="projection")
    finally:
        current_attribution.reset(token)
    assert result == [{"ok": 1}]
    stats = captured[0]
    assert stats["service_id"] == registered[0]["service_id"] == "service-test"
    assert stats["request_id"] == "request-test"
    assert stats["section"] == "dashboard.summary"
    assert stats["section_id"] == sections.timings()[0]["section_id"]
    assert stats["admission_wait_ms"] == pytest.approx(20)
    assert stats["network_ms"] == pytest.approx(50)
    assert stats["decode_ms"] == pytest.approx(30)
    assert stats["client_elapsed_ms"] == pytest.approx(100)
    assert stats["server_cpu_ms"] is None
    assert stats["server_elapsed_ms"] == pytest.approx(4)
    assert stats["server_elapsed_source"] == "statistics.elapsed"
    assert stats["query_id"] == transmitted[0]
    observation = DebugQuery.model_validate(telemetry.get_queries()[-1]).clickhouse
    assert observation.query_id == transmitted[0]
    assert observation.section_id == stats["section_id"]
    assert observation.server_elapsed_ms == pytest.approx(4)


@pytest.mark.parametrize("value", [None, True, "1", -1, float("nan"), float("inf"), 10**500])
def test_invalid_server_elapsed_is_unknown_without_breaking_query(value):
    assert ch._server_elapsed_ms(value) is None


def test_server_elapsed_preserves_measured_zero_and_uses_seconds():
    assert ch._server_elapsed_ms(0) == 0
    assert ch._server_elapsed_ms(0.005) == pytest.approx(5)


def test_pool_rejection_records_wait_without_inventing_network_or_decode(clients, monkeypatch):
    clock = [0.0]
    captured = []
    monkeypatch.setattr(ch.time, "perf_counter", lambda: clock[0])
    monkeypatch.setattr(ch, "_record_operation", lambda **stats: captured.append(stats))

    class Slots:
        def acquire(self, **kwargs):
            clock[0] += 0.02
            return False

        def release(self):
            raise AssertionError("unacquired slot must not be released")

    client = clients(lambda request: pytest.fail("network must not be attempted"))
    client._slots = Slots()
    with pytest.raises(ch.ClickHouseError, match="timeout"):
        client.execute("SELECT 1")
    assert captured[0]["admission_wait_ms"] == pytest.approx(20)
    assert captured[0]["network_ms"] is None
    assert captured[0]["decode_ms"] is None
    assert captured[0]["server_cpu_ms"] is None


def test_telemetry_failure_does_not_break_success(clients, monkeypatch):
    def broken(*args, **kwargs):
        raise RuntimeError("telemetry failed")

    monkeypatch.setattr(ch, "_record_operation", broken)
    assert clients(lambda _: response()).execute("SELECT 1") == [{"ok": 1}]


@pytest.mark.parametrize("body", [b"", b'{"data": {}}', b'{"data": [1]}', b'{"data": []}\nCode: 1, secret'])
def test_invalid_select_response_is_not_empty_success(clients, body):
    with pytest.raises(ch.ClickHouseError):
        clients(lambda _: httpx.Response(200, content=body)).execute("SELECT 1")


def test_exception_header_rejects_even_success_status(clients):
    with pytest.raises(ch.ClickHouseError, match="server error"):
        clients(lambda _: httpx.Response(200, headers={"X-ClickHouse-Exception-Code": "159"})).execute("SELECT 1")


def test_no_identifier_binding_even_with_safe_looking_value(clients):
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
    def handler(request):
        assert query_form(request)["param_value"] == [encoded]
        return response()

    clients(handler).execute("SELECT {value:Nullable(String)}", {"value": value})


def test_configures_httpx_pool_and_timeouts(settings, monkeypatch):
    constructor = MagicMock()
    monkeypatch.setattr(ch.httpx, "Client", constructor)
    client = ch.ClickHouseClient(settings)
    kwargs = constructor.call_args.kwargs
    assert kwargs["limits"].max_connections == settings.pool_max_size
    assert kwargs["limits"].max_keepalive_connections == settings.pool_max_size
    assert kwargs["timeout"].connect == settings.connect_timeout_s
    assert kwargs["timeout"].pool == settings.pool_timeout_s
    assert kwargs["follow_redirects"] is False
    assert kwargs["trust_env"] is False
    client.close()


def test_close_during_request_does_not_interrupt_it(settings):
    entered, release = threading.Event(), threading.Event()

    class Transport(httpx.MockTransport):
        closed = False

        def close(self):
            self.closed = True

    def handler(_):
        entered.set()
        assert release.wait(3)
        return response()

    transport = Transport(handler)
    client = ch.ClickHouseClient(settings, transport=transport)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(client.execute, "SELECT 1")
        try:
            assert entered.wait(3)
            client.close()
            assert not transport.closed
            with pytest.raises(ch.ClickHouseError, match="closed"):
                client.execute("SELECT 1")
        finally:
            release.set()
        assert future.result() == [{"ok": 1}]
    assert transport.closed


def test_singleton_is_threadsafe_and_resettable(settings, monkeypatch):
    monkeypatch.setattr(ch, "_client", None)
    monkeypatch.setattr(ch.config, "load_clickhouse_config", lambda: settings)
    constructor = MagicMock(return_value=MagicMock())
    monkeypatch.setattr(ch, "ClickHouseClient", constructor)
    with ThreadPoolExecutor(max_workers=4) as executor:
        instances = list(executor.map(lambda _: ch.get_clickhouse_client(), range(8)))
    assert all(instance is instances[0] for instance in instances)
    assert constructor.call_count == 1
    ch.close_clickhouse_client()
    instances[0].close.assert_called_once()
    assert ch._client is None


@pytest.mark.parametrize("failure", [None, "health", "startup", "serving"])
def test_app_lifecycle_initializes_probes_and_always_closes(monkeypatch, failure):
    from backend import main

    client = MagicMock()
    if failure == "health":
        client.health.side_effect = ch.ClickHouseError("server error", query_id="test")
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
        patch("backend.core.clickhouse_client.get_clickhouse_client", return_value=client) as initialize,
        patch("backend.core.clickhouse_client.close_clickhouse_client") as close,
    ):

        async def run():
            async with main.lifespan(main.app):
                initialize.assert_called_once()
                client.health.assert_called_once()
                if failure == "serving":
                    raise RuntimeError("serving failure")

        if failure == "startup":
            monkeypatch.setattr(
                main,
                "register_high_scale_services_from_environment",
                MagicMock(side_effect=RuntimeError("startup failure")),
            )
        if failure:
            with pytest.raises((RuntimeError, ch.ClickHouseError)):
                asyncio.run(run())
        else:
            asyncio.run(run())
        close.assert_called_once()


def test_close_wraps_transport_failure(settings):
    class Transport(httpx.MockTransport):
        def close(self):
            raise httpx.CloseError("FAKE_PASSWORD in raw close error")

    client = ch.ClickHouseClient(settings, transport=Transport(lambda _: response()))
    with pytest.raises(ch.ClickHouseError, match="close") as error:
        client.close()
    assert "FAKE_PASSWORD" not in "".join(traceback.format_exception(error.value))
    client.close()


def test_otel_metric_reuses_instrument_and_avoids_high_cardinality(clients, monkeypatch):
    clickhouse_metrics._instrument.cache_clear()
    meter = MagicMock()
    instruments = {}
    meter.create_histogram.side_effect = lambda name, **kwargs: instruments.setdefault(name, MagicMock())
    monkeypatch.setattr(clickhouse_metrics, "get_meter", lambda: meter)
    try:
        client = clients(lambda _: response())
        client.execute("SELECT 1")
        client.execute("SELECT 1")
        assert meter.create_histogram.call_count == 4
        assert set(instruments) == {
            "app.clickhouse_query_duration_ms",
            "app.clickhouse_admission_wait_ms",
            "app.clickhouse_network_ms",
            "app.clickhouse_decode_ms",
        }
        for name, instrument in instruments.items():
            records = instrument.record.call_args_list
            assert len(records) == 2
            expected = {"outcome": "success"}
            if name != "app.clickhouse_query_duration_ms":
                expected["operation"] = "execute"
            assert all(record.args[1] == expected for record in records)
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
    client = ch.ClickHouseClient(replace(settings, host="127.0.0.1", port=server.server_port))
    try:
        assert client.execute("SELECT {value:UInt8} AS ok", {"value": 1}) == [{"ok": 1}]
        client.insert_rows(ch.CLICKHOUSE_FACT_TABLE, ["country"], [("US",)])
        assert observed[0]["headers"]["Content-Type"].startswith("multipart/form-data;")
        assert observed[0]["headers"]["Authorization"].startswith("Basic ")
        assert b'name="param_value"' in observed[0]["body"]
        assert "param_value" not in observed[0]["path"]
        assert json.loads(observed[1]["body"]) == ["US"]
    finally:
        client.close()
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

    client = ch.ClickHouseClient(settings, transport=httpx.MockTransport(handler))
    try:
        if operation == "insert":
            client.insert_rows(ch.CLICKHOUSE_FACT_TABLE, ["country"], [("US",)])
        else:
            client.execute("SELECT 1")
    finally:
        client.close()


@pytest.mark.parametrize("header", ['{"read_rows":"10","read_bytes":"100"}', "invalid", "null"])
def test_summary_headers_are_observational(clients, monkeypatch, header):
    recorded = []
    monkeypatch.setattr(ch, "_record_operation", lambda **stats: recorded.append(stats))
    client = clients(lambda _: httpx.Response(200, headers={"X-ClickHouse-Summary": header}))
    client.insert_rows(ch.CLICKHOUSE_FACT_TABLE, ["country"], [("US",)])
    assert recorded[-1]["rows_written"] == 1
    if header.startswith("{"):
        assert recorded[-1]["rows_read"] == 10
        assert recorded[-1]["bytes_read"] == 100


def test_performance_waterfall_and_scatter_tables_and_columns_are_allowlisted():
    for table in ("performance_minute_summary", "performance_scatter_samples"):
        assert table in ch.CLICKHOUSE_HIGH_SCALE_TABLES
    for column in (
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


def test_cmcd_projection_geography_columns_are_allowlisted_for_insert():
    assert {"country", "asn", "cmcd"} <= ch.CLICKHOUSE_TABLE_COLUMNS["cmcd_projection_facts"]


def test_rum_environment_columns_are_allowlisted_for_both_rum_fact_tables():
    environment = {"browser", "os", "device", "city", "region", "pop", "tls", "ttfb", "country"}
    assert environment <= ch.CLICKHOUSE_TABLE_COLUMNS["rum_vitals_facts"]
    assert environment | {"error_line", "error_col"} <= ch.CLICKHOUSE_TABLE_COLUMNS["rum_error_facts"]


def test_query_settings_travel_as_url_settings_never_inside_the_sql_text(clients):
    sql = "SELECT 1 AS n"
    filters = "{'request_facts': 'service_id = \\'svc\\''}"
    seen = []

    def handler(request):
        seen.append(request)
        assert query_form(request)["query"] == [sql]
        return response([{"n": 1}])

    clients(handler).execute(
        sql, settings={"additional_table_filters": filters, "max_result_rows": "11", "result_overflow_mode": "break"}
    )

    params = seen[0].url.params
    assert params["additional_table_filters"] == filters
    assert params["max_result_rows"] == "11" and params["result_overflow_mode"] == "break"
    assert params["max_memory_usage"] == "1073741824", "the client's own limits stay in force"


@pytest.mark.parametrize(
    "name", ["readonly", "max_memory_usage", "max_execution_time", "query_id", "database", "not_a_setting"]
)
def test_only_allow_listed_query_settings_may_be_supplied(clients, name):
    client = clients(lambda request: pytest.fail("a rejected setting must not reach the server"))

    with pytest.raises(ValueError, match="setting"):
        client.execute("SELECT 1", settings={name: "1"})


def test_credentials_in_user_ddl_never_reach_the_debug_query_telemetry(clients):
    from backend.utils import telemetry

    token = telemetry._QUERIES.set([])
    try:
        clients(lambda request: httpx.Response(200, content=b"")).execute(
            "ALTER USER `u` IDENTIFIED WITH sha256_password BY 'hunter2-super-secret' DEFAULT DATABASE `d`"
        )
        recorded = " ".join(q["sql"] for q in telemetry.get_queries())
    finally:
        telemetry._QUERIES.reset(token)

    assert "hunter2-super-secret" not in recorded
    assert "BY '[REDACTED]'" in recorded


@pytest.fixture
def instruments(monkeypatch):
    metrics._instrument.cache_clear()
    meter = MagicMock()
    found = {}

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


def test_exact_lazy_query_instruments_and_statistics(instruments):
    assert not instruments
    con = client(lambda _: httpx.Response(200, json={"data": [], "statistics": {"bytes_read": 81}}))
    try:
        con.execute("SELECT {value:String}", {"value": "PRIVATE"})
    finally:
        con.close()
    assert set(instruments) == {
        "app.clickhouse_query_duration_ms",
        "app.clickhouse_queries_total",
        "app.clickhouse_bytes_read",
        "app.clickhouse_admission_wait_ms",
        "app.clickhouse_network_ms",
        "app.clickhouse_decode_ms",
    }
    for name in ("admission_wait_ms", "network_ms", "decode_ms"):
        phase, phase_options = instruments[f"app.clickhouse_{name}"]
        assert phase_options["unit"] == "ms"
        assert phase.record.call_args.args[0] >= 0
        assert phase.record.call_args.args[1] == {"operation": "execute", "outcome": "success"}
    histogram, options = instruments["app.clickhouse_query_duration_ms"]
    assert options["unit"] == "ms"
    assert histogram.record.call_args.args[0] >= 0
    assert histogram.record.call_args.args[1] == {"outcome": "success"}
    instruments["app.clickhouse_queries_total"][0].add.assert_called_once_with(1, {"outcome": "success"})
    assert instruments["app.clickhouse_bytes_read"][1]["unit"] == "By"
    instruments["app.clickhouse_bytes_read"][0].add.assert_called_once_with(81)
    assert "PRIVATE" not in str(instruments)


def test_acknowledged_physical_rows_include_retries_not_failed_inserts(instruments):
    replies = iter([httpx.Response(200), httpx.Response(200), httpx.Response(503)])
    con = client(lambda _: next(replies))
    try:
        for _ in range(2):
            con.insert_rows("log_facts", ["country"], [("US",), ("US",)])
        with pytest.raises(ClickHouseError):
            con.insert_rows("log_facts", ["country"], [("US",)])
    finally:
        con.close()
    counter = instruments["app.clickhouse_rows_inserted_total"][0]
    assert [c.args for c in counter.add.call_args_list] == [(2,), (2,)]
    hist, options = instruments["app.clickhouse_insert_duration_ms"]
    assert options["unit"] == "ms"
    assert [c.args[1] for c in hist.record.call_args_list] == [
        {"outcome": "success"},
        {"outcome": "success"},
        {"outcome": "error"},
    ]
    assert "app.clickhouse_queries_total" not in instruments


def test_admission_timeout_is_measured(instruments):
    con = client(lambda _: pytest.fail("admission failed before HTTP"))
    con._slots = MagicMock()
    con._slots.acquire.return_value = False
    try:
        with pytest.raises(ClickHouseError, match="pool"):
            con.execute("SELECT 1")
    finally:
        con.close()
    instruments["app.clickhouse_queries_total"][0].add.assert_called_once_with(1, {"outcome": "timeout"})
    assert instruments["app.clickhouse_query_duration_ms"][0].record.call_args.args[0] >= 0


def test_metric_failure_does_not_mask_transport_error(monkeypatch):
    monkeypatch.setattr(metrics, "_instrument", MagicMock(side_effect=RuntimeError("telemetry broken")))
    con = client(lambda _: httpx.Response(503))
    try:
        with pytest.raises(ClickHouseError, match="server error"):
            con.execute("SELECT 1")
    finally:
        con.close()


@pytest.fixture
def monitor(monkeypatch):
    client, store, meter = MagicMock(), MagicMock(), MagicMock()
    client.storage_health.return_value = {"up": 1, "disk_free": 40, "disk_total": 100}
    store.publication_lag_seconds.return_value = 12.5
    monkeypatch.setattr(observer, "get_meter", lambda: meter)
    monkeypatch.setattr(observer, "PgManifest", lambda: store)
    sampler = observer.ClickHouseObserver()
    sampler.start(client)
    yield sampler, client, store, meter
    sampler.stop()


def test_gauges_are_lazy_and_share_one_cheap_sample(monitor):
    sampler, client, store, meter = monitor
    client.storage_health.assert_not_called()
    names = {call.args[0] for call in meter.create_observable_gauge.call_args_list}
    assert names == {
        "app.clickhouse_up",
        "app.clickhouse_disk_free",
        "app.clickhouse_disk_total",
        "app.clickhouse_publication_lag_seconds",
    }
    assert sampler.observe("up")[0].value == 1
    assert sampler.observe("disk_free")[0].value == 40
    assert sampler.observe("disk_total")[0].value == 100
    assert sampler.observe("lag")[0].value == 12.5
    client.storage_health.assert_called_once_with()
    store.publication_lag_seconds.assert_called_once_with()
    assert all(not item.attributes for item in sampler.observe("lag"))


def test_errors_are_unknown_except_health_zero_and_never_keep_old_values(monitor):
    sampler, client, store, _ = monitor
    assert sampler.observe("lag")[0].value > 0
    client.storage_health.side_effect = RuntimeError("PRIVATE connection")
    store.publication_lag_seconds.side_effect = RuntimeError("PRIVATE DSN")
    sampler._sampled_at = 0
    assert sampler.observe("up")[0].value == 0
    assert sampler.observe("disk_free") == []
    assert sampler.observe("disk_total") == []
    assert sampler.observe("lag") == []
    client.storage_health.side_effect = None
    store.publication_lag_seconds.side_effect = None
    store.publication_lag_seconds.return_value = 0
    sampler._sampled_at = 0
    assert sampler.observe("lag")[0].value == 0
    assert sampler.observe("up")[0].value == 1


def test_stop_disables_callbacks_and_closes_client_restart_reuses_gauges(monitor):
    sampler, client, _, meter = monitor
    sampler.stop()
    sampler.stop()
    assert sampler.observe("up") == []
    client.close.assert_called_once_with()
    sampler.start(client)
    assert meter.create_observable_gauge.call_count == 4
    assert sampler.observe("up")[0].value == 1


def test_disabled_does_not_create_client_or_gauges(monkeypatch):
    monkeypatch.setattr(observer.config, "load_clickhouse_config", lambda: None)
    make = MagicMock(side_effect=AssertionError("opened disabled client"))
    monkeypatch.setattr(observer, "ClickHouseClient", make)
    observer.start_clickhouse_observer()
    make.assert_not_called()


def _rows_from_fixture(con) -> list[tuple]:
    out = []
    for idx, row in enumerate(con.execute("SELECT * FROM fixture").fetch_arrow_table().to_pylist()):
        row["row_ordinal"] = idx
        out.append(canonical_row(row))
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


def test_export_roundtrip_stable_ordinals_and_duplicates(fixture):
    dataset, objects, fos, con = fixture
    all_rows = _rows_from_fixture(con)
    artifacts = [objects.put(dataset, tuple(all_rows[i : i + 2])) for i in range(0, len(all_rows), 2)]
    rows = tuple(row for artifact in artifacts for row in objects.load(artifact).rows)
    assert [row[1] for row in rows] == list(range(5))
    assert digest_rows(rows) == digest_rows(tuple(all_rows))
    assert len(artifacts) == 3
    assert fos.puts == 3
    assert all("/clickhouse-prototype/" in a.artifact_uri for a in artifacts)
    assert rows[0][3] is None


def test_corrupt_artifact_or_wrong_bucket_rejected(fixture):
    dataset, objects, fos, con = fixture
    all_rows = _rows_from_fixture(con)
    artifact = objects.put(dataset, tuple(all_rows))
    with pytest.raises(ValueError, match="bucket"):
        objects.load(replace(artifact, artifact_uri=artifact.artifact_uri.replace("test-bucket", "other-bucket")))
    key = next(iter(fos.objects))
    payload, metadata = fos.objects[key]
    fos.objects[key] = (b"x" + payload[1:], metadata)
    with pytest.raises(ValueError, match="hash"):
        objects.load(artifact)


def test_head_mismatch_never_seals(fixture):
    dataset, objects, fos, con = fixture
    fos.head_object = lambda **kwargs: {"ContentLength": 0, "Metadata": {}}
    all_rows = _rows_from_fixture(con)
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
def test_cli_dryrun_never_opens_storage(monkeypatch, mode):
    no_storage = MagicMock(side_effect=AssertionError("must not open storage"))
    for name in ("PgManifest", "FosArtifacts"):
        monkeypatch.setattr(f"scripts.clickhouse_replay.{name}", no_storage)
    args = parser().parse_args(["--service-id", "TestPrototype", *mode])
    assert run(args)["dry_run"]
    no_storage.assert_not_called()


@pytest.mark.parametrize("extra", [["--limit", "0"], ["--max-rows", "1000001"], ["--ttl-hours", "nan"]])
def test_cli_rejects_unbounded_arguments(extra):
    args = parser().parse_args(
        ["--service-id", "TestPrototype", "--export", "--start", "2026-09-01", "--end", "2026-09-02", *extra]
    )
    with pytest.raises(ValueError):
        run(args)


def test_cli_flushes_metrics_on_success_and_failure(monkeypatch):
    events = []
    monkeypatch.setattr("sys.argv", ["replay", "--service-id", "TestPrototype", "--rebuild", "fixture"])
    monkeypatch.setattr(clickhouse_replay, "force_flush", lambda: events.append("flush"))
    monkeypatch.setattr(clickhouse_replay, "close_clickhouse_client", lambda: events.append("close"))
    monkeypatch.setattr(clickhouse_replay, "run", lambda args: {"dry_run": True})
    assert clickhouse_replay.main() == 0
    assert events == ["flush", "close"]
    events.clear()

    def fail(args):
        raise ValueError("PRIVATE")

    monkeypatch.setattr(clickhouse_replay, "run", fail)
    assert clickhouse_replay.main() == 1
    assert events == ["flush", "close"]


# ── Merged from tests/core/test_clickhouse_integration.py ──
class AmbiguousClient:
    def __init__(self, client, partial=False):
        self.client = client
        self.partial = partial

    def execute(self, *args, **kwargs):
        return self.client.execute(*args, **kwargs)

    def insert_rows(self, table, columns, rows):
        self.client.insert_rows(table, columns, rows[:1] if self.partial else rows)
        raise RuntimeError("simulated lost acknowledgement")


@unittest.skipUnless(os.getenv("RUN_CLICKHOUSE_INTEGRATION") == "1", "explicit real-engine gate")
class PublicationIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = get_clickhouse_client()
        assert cls.client is not None
        create_clickhouse_schema(cls.client)
        create_clickhouse_schema(cls.client)
        cls.store = PgManifest()

    @classmethod
    def tearDownClass(cls):
        cls.client.close()
        cls.store.pool.close()

    def setUp(self):
        self.service = "TestPrototype" + uuid4().hex
        rows = tuple(
            ("raw/placeholder.gz", n, "2026-09-01 00:00:00.123456", None, "192.0.2.1", "/", 2) for n in range(3)
        )
        self.dataset = Dataset(
            self.service,
            uuid4().hex,
            "test-catalog",
            "logs_fixture",
            1,
            datetime(2026, 9, 1, tzinfo=UTC),
            datetime(2026, 9, 2, tzinfo=UTC),
            datetime.now(UTC) + timedelta(hours=1),
            len(rows),
            digest_rows(rows),
        )
        content = sha256(b"fixture immutable bytes").hexdigest()
        self.artifact = Artifact(
            self.service,
            self.dataset.dataset_id,
            _rows_build_batch_id(self.service, self.dataset.dataset_id, content),
            "s3://test-bucket/clickhouse-prototype/fixture.parquet",
            content,
            digest_rows(rows),
            100,
            0,
            len(rows),
        )
        self.batch = DurableBatch(self.artifact, rows)
        self.store.seal(self.dataset, [self.artifact])
        self.generation = self.store.begin_generation(
            self.service, self.dataset.dataset_id, target_identity(self.client)
        )

    def tearDown(self):
        self.client.execute(
            "ALTER TABLE log_facts DELETE WHERE service_id={service:String} SETTINGS mutations_sync=1",
            {"service": self.service},
        )
        with self.store.transaction() as con:
            for table in (
                "clickhouse_service_selection",
                "clickhouse_publications",
                "clickhouse_generations",
                "clickhouse_artifacts",
                "clickhouse_datasets",
            ):
                con.execute(f"DELETE FROM {table} WHERE service_id=%s", (self.service,))

    def count(self, generation=None):
        return self.client.execute(
            "SELECT count() AS n FROM log_facts FINAL WHERE service_id={service:String} "
            "AND generation={generation:String}",
            {"service": self.service, "generation": generation or self.generation},
        )[0]["n"]

    def test_admin_status_and_preview_are_service_scoped_read_only(self):
        target = target_identity(self.client)
        store = PgManifest(bounded=True)
        before = store.admin_status(self.service, target)
        self.assertEqual(before["schema_version"], 1)
        self.assertEqual(before["publication_counts"], {"pending": 1, "claimed": 0, "failed": 0, "published": 0})
        self.assertIsNotNone(before["oldest_pending_age_seconds"])
        self.assertIsNone(before["active_generation"])
        empty = store.admin_status("TestMissing" + uuid4().hex, target)
        self.assertEqual(sum(empty["publication_counts"].values()), 0)
        self.assertIsNone(empty["oldest_pending_age_seconds"])
        self.assertIsNone(empty["last_published_at"])
        for generation in (None, self.generation):
            preview = store.replay_preview(
                self.service,
                dataset_id=None if generation else self.dataset.dataset_id,
                generation=generation,
                target=target,
                limit=1,
            )
            self.assertEqual(
                preview, {"dataset_id": self.dataset.dataset_id, "artifact_count": 1, "planned_artifacts": 1}
            )
        with self.assertRaises(LookupError):
            store.replay_preview(
                "TestOther", dataset_id=self.dataset.dataset_id, generation=None, target=target, limit=1
            )
        after = store.admin_status(self.service, target)
        self.assertEqual(before["publication_counts"], after["publication_counts"])
        self.assertEqual(self.count(), 0)

    def test_admin_status_retains_expired_selection_without_claiming_freshness(self):
        result = replay_unpublished(
            self.service,
            generation=self.generation,
            loader=lambda artifact: self.batch,
            store=self.store,
            client=self.client,
        )
        self.assertTrue(result.activated)
        target = target_identity(self.client)
        status = self.store.admin_status(self.service, target)
        self.assertEqual(status["publication_counts"]["published"], 1)
        self.assertIsNotNone(status["last_published_at"])
        self.assertIsNone(status["oldest_pending_age_seconds"])
        self.assertFalse(status["active_generation"]["expired"])
        self.assertTrue(status["active_generation"]["target_matches"])
        with self.store.transaction() as con:
            con.execute(
                "UPDATE clickhouse_datasets SET expires_at=clock_timestamp()-interval '1 second' WHERE service_id=%s",
                (self.service,),
            )
        status = self.store.admin_status(self.service, target)
        self.assertTrue(status["active_generation"]["expired"])
        self.assertEqual(status["expired_dataset_count"], 1)
        with self.assertRaises(ValueError):
            self.store.replay_preview(
                self.service, dataset_id=self.dataset.dataset_id, generation=None, target=target, limit=1
            )

    def test_observable_lag_excludes_terminal_expired_and_superseded_generations(self):
        # Force this fixture to be older than any bounded production backlog;
        # no other tenant's rows are changed.
        with self.store.transaction() as con:
            con.execute(
                "UPDATE clickhouse_publications SET created_at=clock_timestamp()-interval '100 years' "
                "WHERE service_id=%s",
                (self.service,),
            )
        for status in ("pending", "claimed", "failed"):
            with self.store.transaction() as con:
                con.execute("UPDATE clickhouse_publications SET status=%s WHERE service_id=%s", (status, self.service))
            self.assertGreater(self.store.publication_lag_seconds(), 3_000_000_000)
        for exclude, restore in (
            (
                "UPDATE clickhouse_publications SET status='published' WHERE service_id=%s",
                "UPDATE clickhouse_publications SET status='pending' WHERE service_id=%s",
            ),
            (
                "UPDATE clickhouse_generations SET status='retired' WHERE service_id=%s",
                "UPDATE clickhouse_generations SET status='building' WHERE service_id=%s",
            ),
            (
                "UPDATE clickhouse_service_selection SET revision=revision+1 WHERE service_id=%s",
                "UPDATE clickhouse_service_selection SET revision=revision-1 WHERE service_id=%s",
            ),
            (
                "UPDATE clickhouse_datasets SET expires_at=clock_timestamp()-interval '1 second' WHERE service_id=%s",
                "UPDATE clickhouse_datasets SET expires_at=clock_timestamp()+interval '1 hour' WHERE service_id=%s",
            ),
        ):
            with self.store.transaction() as con:
                con.execute(exclude, (self.service,))
            self.assertLess(self.store.publication_lag_seconds(), 3_000_000_000)
            with self.store.transaction() as con:
                con.execute(restore, (self.service,))

    def publish(self, client=None):
        return publish_batch(
            self.service, self.batch, generation=self.generation, store=self.store, client=client or self.client
        )

    def ready(self, client=None):
        return readiness(
            self.service,
            start=self.dataset.coverage_start,
            end=self.dataset.coverage_end,
            source_snapshot=1,
            catalog_identity="test-catalog",
            source_table="logs_fixture",
            store=self.store,
            client=client or self.client,
        )

    def test_accepted_insert_lost_ack_retry_exact_before_merge(self):
        with self.assertRaises(RuntimeError):
            self.publish(AmbiguousClient(self.client))
        self.assertEqual(self.count(), 3)
        self.assertEqual(self.ready().reason, "no_active_generation")
        self.assertFalse(self.store.activate(self.service, self.generation, target_identity(self.client)))
        self.assertEqual(self.publish().status, "published")
        self.assertEqual(self.count(), 3)
        self.assertTrue(activate_generation(self.service, self.generation, store=self.store, client=self.client))
        self.assertTrue(self.ready().eligible)
        self.assertEqual(self.publish().status, "not_claimed")

    def test_partial_insert_retry_and_no_incomplete_visibility(self):
        with self.assertRaises(RuntimeError):
            self.publish(AmbiguousClient(self.client, partial=True))
        self.assertEqual(self.count(), 1)
        self.assertFalse(self.ready().eligible)
        self.assertEqual(self.publish().status, "published")
        self.assertEqual(self.count(), 3)

    def test_stale_claimant_is_fenced_and_expired_claim_cannot_finish(self):
        old = self.store.claim(self.service, self.generation, self.artifact.batch_id)
        self.assertIsNone(self.store.claim(self.service, self.generation, self.artifact.batch_id))
        with self.store.transaction() as con:
            con.execute(
                "UPDATE clickhouse_publications SET lease_until=clock_timestamp()-interval '1 second' "
                "WHERE service_id=%s",
                (self.service,),
            )
        self.assertFalse(
            self.store.finish(
                self.service, self.generation, self.artifact.batch_id, old, digest=self.artifact.canonical_digest
            )
        )
        new = self.store.claim(self.service, self.generation, self.artifact.batch_id)
        self.assertEqual(new, old + 1)
        self.assertFalse(
            self.store.finish(
                self.service, self.generation, self.artifact.batch_id, old, digest=self.artifact.canonical_digest
            )
        )

    def test_crash_after_insert_before_postgres_update(self):
        with patch.object(self.store, "finish", side_effect=SystemExit):
            with self.assertRaises(SystemExit):
                self.publish()
        self.assertEqual(self.count(), 3)
        self.assertIsNone(self.store.selected(self.service))
        with self.store.transaction() as con:
            con.execute(
                "UPDATE clickhouse_publications SET lease_until=clock_timestamp() WHERE service_id=%s", (self.service,)
            )
        self.assertEqual(self.publish().status, "published")
        self.assertEqual(self.count(), 3)

    def test_full_rebuild_does_not_skip_old_publications_or_empty_target(self):
        self.publish()
        self.assertTrue(activate_generation(self.service, self.generation, store=self.store, client=self.client))
        self.client.execute(
            "ALTER TABLE log_facts DELETE WHERE service_id={service:String} SETTINGS mutations_sync=1",
            {"service": self.service},
        )
        self.assertEqual(self.ready().reason, "target_unverified")
        result = full_rebuild(
            self.service,
            self.dataset.dataset_id,
            loader=lambda artifact: self.batch,
            store=self.store,
            client=self.client,
        )
        self.assertNotEqual(result.generation, self.generation)
        self.assertTrue(result.activated)
        self.assertEqual(result.published, 1)
        self.assertEqual(self.count(result.generation), 3)
        self.assertTrue(self.ready().eligible)

    def test_replacement_table_uuid_rejected_and_rebuilds(self):
        self.publish()
        activate_generation(self.service, self.generation, store=self.store, client=self.client)
        # This database name is internally generated, not operator/user SQL.
        name = "test_prototype_" + uuid4().hex
        self.client.execute(f"CREATE DATABASE {name}")
        other = ClickHouseClient(replace(self.client._settings, database=name))
        identity = None
        try:
            create_clickhouse_schema(other)
            identity = target_identity(other)
            self.assertEqual(self.ready(other).reason, "target_unverified")
            result = full_rebuild(
                self.service,
                self.dataset.dataset_id,
                loader=lambda artifact: self.batch,
                store=self.store,
                client=other,
            )
            self.assertTrue(result.activated)
            self.assertTrue(self.ready(other).eligible)
        finally:
            other.close()
            self.client.execute(f"DROP DATABASE {name}")
            if identity:
                with self.store.transaction() as con:
                    con.execute("DELETE FROM clickhouse_schema_state WHERE target_identity=%s", (identity,))

    def test_late_complete_generation_cannot_replace_new_selection(self):
        second = self.store.begin_generation(self.service, self.dataset.dataset_id, target_identity(self.client))
        self.publish()
        publish_batch(self.service, self.batch, generation=second, store=self.store, client=self.client)
        self.assertTrue(activate_generation(self.service, second, store=self.store, client=self.client))
        self.assertFalse(activate_generation(self.service, self.generation, store=self.store, client=self.client))
        self.assertEqual(self.store.selected(self.service)["generation"], second)

    def test_real_transaction_rolls_back_and_manifest_is_immutable(self):
        with self.assertRaises(RuntimeError), self.store.transaction() as con:
            con.execute("UPDATE clickhouse_datasets SET expected_rows=0 WHERE service_id=%s", (self.service,))
            raise RuntimeError("rollback")
        self.assertEqual(self.store.dataset(self.service, self.dataset.dataset_id).expected_rows, 3)
        with self.assertRaises(Exception):
            self.store.seal(self.dataset, [self.artifact])
        self.assertEqual(len(self.store.artifacts(self.service, self.dataset.dataset_id)), 1)

    def test_wrong_content_same_count_is_not_published(self):
        class WrongClient(AmbiguousClient):
            def insert_rows(inner, table, columns, rows):
                wrong = [(*row[:6], "WRONG", *row[7:]) for row in rows]
                inner.client.insert_rows(table, columns, wrong)

        with self.assertRaises(RuntimeError):
            self.publish(WrongClient(self.client))
        self.assertEqual(self.count(), 3)
        self.assertFalse(self.store.activate(self.service, self.generation, target_identity(self.client)))

    def test_exporter_seals_real_postgres_then_rebuilds_from_tiny_fos(self):
        objects = FosArtifacts({"service_id": self.service, "bucket": "test-bucket"}, client=TinyFos())
        dataset = replace(self.dataset, dataset_id=uuid4().hex)
        with duckdb.connect() as con:
            raw_rows = (
                con.execute(
                    "SELECT 'source' AS source_identity, TIMESTAMP '2026-09-01' AS timestamp, "
                    "NULL::VARCHAR AS country, '192.0.2.1' AS ip, '/' AS url, 1::BIGINT AS conn_requests FROM range(5)"
                )
                .fetch_arrow_table()
                .to_pylist()
            )
        rows = []
        for idx, row in enumerate(raw_rows):
            row["row_ordinal"] = idx
            rows.append(canonical_row(row))
        artifacts = [objects.put(dataset, tuple(rows[i : i + 2])) for i in range(0, len(rows), 2)]
        exported = replace(dataset, expected_rows=len(rows), canonical_digest=digest_rows(tuple(rows)))
        self.store.seal(exported, artifacts)
        self.assertEqual(exported.expected_rows, 5)
        partial = full_rebuild(
            self.service, exported.dataset_id, loader=objects.load, limit=1, store=self.store, client=self.client
        )
        self.assertEqual(partial.published, 1)
        self.assertFalse(partial.activated)
        self.assertIsNone(self.store.selected(self.service))
        complete = replay_unpublished(
            self.service,
            generation=partial.generation,
            loader=objects.load,
            limit=2,
            store=self.store,
            client=self.client,
        )
        self.assertTrue(complete.activated)
        self.assertEqual(self.count(complete.generation), 5)

    def test_expiry_rejects_claim_replay_and_readiness(self):
        self.publish()
        activate_generation(self.service, self.generation, store=self.store, client=self.client)
        with self.store.transaction() as con:
            con.execute(
                "UPDATE clickhouse_datasets SET expires_at=clock_timestamp() WHERE service_id=%s", (self.service,)
            )
        self.assertEqual(self.ready().reason, "expired_or_unsupported_dataset")
        with self.assertRaisesRegex(ValueError, "expired"):
            full_rebuild(
                self.service,
                self.dataset.dataset_id,
                loader=lambda artifact: self.batch,
                store=self.store,
                client=self.client,
            )

    def test_snapshot_and_coverage_mismatch_never_eligible(self):
        self.publish()
        activate_generation(self.service, self.generation, store=self.store, client=self.client)
        kwargs = dict(
            start=self.dataset.coverage_start,
            end=self.dataset.coverage_end,
            source_snapshot=2,
            catalog_identity="test-catalog",
            source_table="logs_fixture",
            store=self.store,
            client=self.client,
        )
        self.assertEqual(readiness(self.service, **kwargs).reason, "source_snapshot_mismatch")
        kwargs["start"] -= timedelta(days=1)
        self.assertEqual(readiness(self.service, **kwargs).reason, "outside_coverage")


if __name__ == "__main__":
    unittest.main()


# ── Merged from tests/core/test_clickhouse_manifest.py ──
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


def test_admin_manifest_transactions_bound_pool_and_statements(manifest):
    store, con = manifest
    store.bounded = True
    with store.transaction():
        pass
    store.pool.connection.assert_called_once_with(timeout=2)
    assert [c.args[0] for c in con.execute.call_args_list] == [
        "SET LOCAL statement_timeout = '2s'",
        "SET LOCAL lock_timeout = '2s'",
    ]


def test_admin_status_projects_only_safe_service_scoped_metadata(manifest):
    store, con = manifest
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
    assert status["schema_version"] == 1
    assert status["publication_counts"] == {"pending": 0, "claimed": 0, "failed": 2, "published": 0}
    assert status["oldest_pending_age_seconds"] == 23
    assert status["last_published_at"] == now.isoformat()
    assert status["active_generation"]["expired"] is True
    assert status["active_generation"]["target_matches"] is False
    assert "target_identity" not in status["active_generation"]
    store.pool.connection.assert_called_once_with(timeout=2)
    selects = [c for c in con.execute.call_args_list if c.args[0].startswith("SELECT")]
    assert selects[0].args[1] == ("target",)
    for call in selects[1:]:
        assert call.args[1] == ("service",)
        assert "artifact_uri" not in call.args[0] and "first_error" not in call.args[0]


@pytest.mark.parametrize("generation", [None, "g"])
def test_replay_preview_only_reads_and_checks_service_references(manifest, generation):
    store, con = manifest
    dataset, _, _ = fixture_manifest()
    rows = []
    if generation:
        rows.append(
            {"dataset_id": "dataset", "status": "building", "target_identity": "target", "current_revision": True}
        )
    rows.extend([{"expires_at": dataset.expires_at, "schema_version": CLICKHOUSE_SCHEMA_VERSION}, {"n": 3}])
    if generation:
        rows.append({"n": 2})
    con.execute.return_value.fetchone.side_effect = rows
    assert store.replay_preview(
        "service", dataset_id=None if generation else "dataset", generation=generation, target="target", limit=1
    ) == {"dataset_id": "dataset", "artifact_count": 3, "planned_artifacts": 1}
    store.pool.connection.assert_called_once_with(timeout=2)
    assert con.execute.call_args_list[0].args[0] == "SET TRANSACTION READ ONLY"
    for call in con.execute.call_args_list[2:]:
        assert call.args[0].startswith("SELECT")
        assert call.args[1][0] == "service"


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


def test_publication_lag_is_bounded_actionable_metadata_only(manifest):
    store, con = manifest
    result(con, {"lag_seconds": 42.5})
    assert store.publication_lag_seconds() == 42.5
    store.pool.connection.assert_called_once_with(timeout=2)
    sql = con.execute.call_args.args[0]
    assert "p.created_at" in sql and "MAX(" in sql
    assert "p.status IN ('pending', 'claimed', 'failed')" in sql
    assert "g.status='building'" in sql
    assert "d.expires_at>clock_timestamp()" in sql
    assert "s.revision=g.selection_revision" in sql
    assert "s.service_id=g.service_id" in sql
    assert "g.service_id=p.service_id" in sql
    assert "g.generation=p.generation" in sql
    assert "d.dataset_id=g.dataset_id" in sql
    assert any("statement_timeout" in call.args[0] for call in con.execute.call_args_list)
    result(con, {"lag_seconds": None})
    assert store.publication_lag_seconds() == 0
    con.execute.side_effect = RuntimeError("offline")
    with pytest.raises(RuntimeError, match="offline"):
        store.publication_lag_seconds()


def test_dataset_expiry_version_and_missing_are_explicit(manifest):
    store, con = manifest
    dataset, _, _ = fixture_manifest()
    result(con, asdict(dataset))
    assert store.dataset(dataset.service_id, dataset.dataset_id) == dataset
    assert con.execute.call_args.args[1] == (dataset.service_id, dataset.dataset_id)
    result(con)
    with pytest.raises(ValueError, match="not found"):
        store.dataset(dataset.service_id, dataset.dataset_id)
    result(con, asdict(replace(dataset, expires_at=datetime.now(UTC) - timedelta(seconds=1))))
    with pytest.raises(ValueError, match="expired"):
        store.dataset(dataset.service_id, dataset.dataset_id)
    result(con, asdict(replace(dataset, schema_version=99)))
    with pytest.raises(ValueError, match="unsupported"):
        store.dataset(dataset.service_id, dataset.dataset_id)


def test_generation_and_artifact_reads_are_service_scoped(manifest, monkeypatch):
    store, con = manifest
    dataset, artifact, _ = fixture_manifest()
    result(con, {"dataset_id": dataset.dataset_id})
    assert store.generation(dataset.service_id, "generation") == {"dataset_id": dataset.dataset_id}
    assert con.execute.call_args.args[1] == (dataset.service_id, "generation")
    result(con)
    with pytest.raises(ValueError, match="generation not found"):
        store.generation(dataset.service_id, "missing")
    monkeypatch.setattr(store, "generation", lambda *args: {"dataset_id": dataset.dataset_id})
    monkeypatch.setattr(store, "dataset", lambda *args: dataset)
    result(con, asdict(artifact))
    assert store.artifact(dataset.service_id, "generation", artifact.batch_id) == artifact
    assert con.execute.call_args.args[1] == (dataset.service_id, dataset.dataset_id, artifact.batch_id)
    result(con)
    with pytest.raises(ValueError, match="not in generation"):
        store.artifact(dataset.service_id, "generation", "missing")
    result(con, rows=[asdict(artifact)])
    assert store.artifacts(dataset.service_id, dataset.dataset_id) == [artifact]
    assert "ORDER BY ordinal_start" in con.execute.call_args.args[0]


def test_generation_uses_locked_selection_revision_and_all_artifacts(manifest, monkeypatch):
    store, con = manifest
    dataset, _, _ = fixture_manifest()
    monkeypatch.setattr(store, "dataset", lambda *args: dataset)
    result(con, {"revision": 4})
    generation = store.begin_generation(dataset.service_id, dataset.dataset_id, "target")
    assert len(generation) == 32
    calls = con.execute.call_args_list
    assert "FOR UPDATE" in calls[1].args[0]
    assert calls[2].args[1] == (dataset.service_id, generation, dataset.dataset_id, "target", 4)
    assert "FROM clickhouse_artifacts" in calls[3].args[0]
    assert "published" not in calls[3].args[0]


def test_claim_is_atomic_bounded_and_finish_checks_live_fence(manifest):
    store, con = manifest
    result(con, {"lease_fence": 3})
    assert store.claim("service", "generation", "batch") == 3
    sql, params = con.execute.call_args.args
    assert "lease_fence=lease_fence+1" in sql
    assert "g.status='building'" in sql and "d.expires_at>clock_timestamp()" in sql
    assert params == (120, "service", "generation", "batch")
    result(con)
    assert store.claim("service", "generation", "batch") is None
    with pytest.raises(ValueError):
        store.claim("service", "generation", "batch", lease_seconds=301)
    assert store.finish("service", "generation", "batch", 3, digest="digest")
    sql, params = con.execute.call_args.args
    assert "p.lease_fence=%s" in sql and "p.lease_until>clock_timestamp()" in sql
    assert params[0] == "published" and params[-1] == 3
    result(con, count=0)
    assert not store.finish("service", "generation", "batch", 2, digest=None, error="TimeoutError")
    assert con.execute.call_args.args[1][0] == "failed"


@pytest.mark.parametrize(
    "scenario,activated", [("valid", True), ("missing", False), ("revision", False), ("pending", False)]
)
def test_activation_requires_complete_membership_locked_revision_and_live_target(manifest, scenario, activated):
    store, con = manifest
    dataset, _, _ = fixture_manifest()
    gen = {
        "status": "building",
        "target_identity": "target",
        "selection_revision": 3,
        "expires_at": dataset.expires_at,
        "dataset_id": dataset.dataset_id,
    }
    if scenario == "revision":
        gen["selection_revision"] = 2
    rows = [None if scenario == "missing" else {"revision": 3}, gen, {"n": int(scenario == "pending")}]

    def execute(sql, params):
        cursor = MagicMock()
        if sql.startswith("SELECT"):
            cursor.fetchone.return_value = rows.pop(0)
        return cursor

    con.execute.side_effect = execute
    assert store.activate(dataset.service_id, "generation", "target") is activated
    assert "FOR UPDATE" in con.execute.call_args_list[0].args[0]
    if activated:
        assert len(con.execute.call_args_list) == 6
        assert "revision=revision+1" in con.execute.call_args.args[0]


def test_pending_and_selection_reads_are_bounded(manifest):
    store, con = manifest
    result(con, rows=[{"batch_id": "b"}])
    assert store.pending("s", "g", 1) == ["b"]
    assert con.execute.call_args.args[1] == ("s", "g", 1)
    with pytest.raises(ValueError):
        store.pending("s", "g", 1001)
    result(con)
    assert store.selected("s") is None
    result(con, {"generation": "g"})
    assert store.selected("s") == {"generation": "g"}


# ── Merged from tests/core/test_clickhouse_schema.py ──
def test_fact_schema_and_deduplication_key():
    ddl = schema.CLICKHOUSE_FACT_DDL
    assert "ReplacingMergeTree" in ddl
    assert "PARTITION BY (service_id, generation)" in ddl
    assert "ORDER BY (service_id, generation, batch_id, row_ordinal)" in ddl
    assert "DateTime64(6, 'UTC')" in ddl
    assert all(f"    {column} " in ddl for column in CLICKHOUSE_FACT_COLUMNS)
    assert "TTL" not in ddl


def test_postgres_manifest_is_additive_and_service_scoped():
    statements = pg_schema_statements()
    for name in ("datasets", "artifacts", "generations", "publications", "service_selection", "schema_state"):
        assert any(f"CREATE TABLE IF NOT EXISTS clickhouse_{name} (" in s for s in statements)
    assert "lease_fence BIGINT" in "\n".join(statements)
    assert "expires_at TIMESTAMPTZ NOT NULL" in "\n".join(statements)


def test_init_repeats_and_checks_actual_schema(monkeypatch):
    monkeypatch.setenv("METADATA_DSN", "postgresql://placeholder")
    client = MagicMock()
    monkeypatch.setattr(schema, "target_identity", lambda c: "target")
    record = MagicMock()
    monkeypatch.setattr(schema, "_record_schema", record)
    maintain = MagicMock()
    monkeypatch.setattr(schema, "maintain_clickhouse_system_tables", maintain)
    schema.create_clickhouse_schema(client)
    schema.create_clickhouse_schema(client)
    assert client.execute.call_count == 2
    client.execute.assert_called_with(schema.CLICKHOUSE_FACT_DDL)
    assert maintain.call_count == 2
    assert record.call_count == 2


def test_maintain_clickhouse_system_tables_executes_truncates_and_ttl_alters():
    client = MagicMock()
    schema.maintain_clickhouse_system_tables(client)
    assert client.execute.call_count == 9
    queries = [call.args[0] for call in client.execute.call_args_list]
    assert any("TRUNCATE TABLE IF EXISTS system.trace_log" in q for q in queries)
    assert any("ALTER TABLE system.query_log MODIFY TTL" in q for q in queries)


def test_reject_sqlite_before_ddl(monkeypatch):
    monkeypatch.delenv("METADATA_DSN", raising=False)
    client = MagicMock()
    with pytest.raises(RuntimeError, match="Postgres"):
        schema.create_clickhouse_schema(client)
    client.execute.assert_not_called()


def test_schema_validation_rejects_wrong_engine():
    client = MagicMock()
    client.execute.return_value = [{"uuid": "target", "engine": "MergeTree"}]
    with pytest.raises(RuntimeError, match="schema"):
        schema.target_identity(client)


def test_schema_validation_checks_uuid_columns_and_exact_keys():
    client = MagicMock()
    tables = [
        {
            "uuid": "target",
            "engine": "ReplacingMergeTree",
            "sorting_key": "service_id, generation, batch_id, row_ordinal",
            "partition_key": "(service_id, generation)",
        }
    ]
    columns = [{"name": name, "type": kind} for name, kind in schema.FACT_TYPES]
    client.execute.side_effect = [tables, columns]
    assert schema.target_identity(client) == "target"
    client.execute.side_effect = [tables, columns[:-1]]
    with pytest.raises(RuntimeError, match="columns mismatch"):
        schema.target_identity(client)


def test_schema_marker_uses_real_transaction_and_serializes_bootstrap(monkeypatch):
    monkeypatch.setenv("METADATA_DSN", "postgresql://placeholder")
    pool, con = MagicMock(), MagicMock()
    pool.connection.return_value.__enter__.return_value = con
    monkeypatch.setattr(schema.pg_connection, "get_pg_pool", lambda: pool)
    schema._record_schema("target")
    con.transaction.assert_called_once()
    assert "pg_advisory_xact_lock" in con.execute.call_args_list[0].args[0]
    assert con.execute.call_args.args[1] == ("target", schema.CLICKHOUSE_SCHEMA_VERSION)


def test_cli_disabled_and_cleanup(monkeypatch):
    monkeypatch.setattr(schema, "get_clickhouse_client", lambda: None)
    with pytest.raises(RuntimeError, match="disabled"):
        schema.main()
    client = MagicMock()
    pool = MagicMock()
    monkeypatch.setattr(schema, "get_clickhouse_client", lambda: client)
    monkeypatch.setattr(schema, "create_clickhouse_schema", lambda c: None)
    monkeypatch.setattr(schema.pg_connection, "is_postgres", lambda: True)
    monkeypatch.setattr(schema.pg_connection, "get_pg_pool", lambda: pool)
    schema.main()
    client.close.assert_called_once()
    pool.close.assert_called_once()
