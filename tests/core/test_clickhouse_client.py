"""ClickHouse HTTP boundary: no server, schema, or serving-path changes."""

from __future__ import annotations

import asyncio
import json
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime
from email.parser import BytesParser
from email.policy import default
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import MagicMock, patch
from uuid import UUID

import httpx
import pytest

from backend import config
from backend.core import clickhouse_client as ch
from backend.core import clickhouse_metrics
from backend.utils import telemetry


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
            # The next uncaught operation after dependency init is the clock stamp.
            monkeypatch.setattr(main, "datetime", MagicMock(now=MagicMock(side_effect=RuntimeError("startup failure"))))
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
    monkeypatch.setattr(clickhouse_metrics, "get_meter", lambda: meter)
    try:
        client = clients(lambda _: response())
        client.execute("SELECT 1")
        client.execute("SELECT 1")
        assert meter.create_histogram.call_count == 1
        records = meter.create_histogram.return_value.record.call_args_list
        assert len(records) == 2
        assert records[0].args[1] == {"outcome": "success"}
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
