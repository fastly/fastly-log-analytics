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
from backend.models.common import DebugQuery
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


# ── Merged from test_clickhouse_metrics.py ──


import pytest

from backend.core import clickhouse_metrics as metrics
from backend.core.clickhouse_client import ClickHouseClient, ClickHouseError


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


# ── Merged from test_clickhouse_observer.py ──


import pytest

from backend.core import clickhouse_observer as observer


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


# ── Merged from test_clickhouse_export.py ──

from datetime import UTC, datetime, timedelta

import duckdb
import pytest

from backend.core.clickhouse_export import FosArtifacts
from backend.core.clickhouse_manifest import Dataset
from backend.core.clickhouse_rows import canonical_row, digest_rows
from scripts import clickhouse_replay
from scripts.clickhouse_replay import parser, run
from tests.core.clickhouse_fos_fixture import TinyFos


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
