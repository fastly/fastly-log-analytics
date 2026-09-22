from unittest.mock import MagicMock

import httpx
import pytest

from backend import config
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
    }
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
