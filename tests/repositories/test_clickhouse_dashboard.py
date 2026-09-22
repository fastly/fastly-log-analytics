"""Narrow hybrid dashboard contract; real DuckDB, a bounded fake CH boundary."""

from datetime import UTC, datetime
from importlib import import_module
from unittest.mock import Mock

import duckdb
import pytest

START = datetime(2026, 9, 1, tzinfo=UTC)
END = datetime(2026, 9, 2, tzinfo=UTC)
SOURCE = {"name": "TestHybrid", "service_id": "TestHybrid"}


def adapter():
    from backend.repositories import dashboard

    assert hasattr(dashboard, "get_aggregates")
    return import_module("backend.repositories.clickhouse_dashboard")


class FakeClickHouse:
    """Execute the emitted predicates and aggregates with real DuckDB."""

    def __init__(self, con):
        self.con = con
        self.calls = []

    def execute(self, sql, params):
        self.calls.append((sql, params))
        assert "FROM log_facts FINAL" in sql
        assert params["service"] == SOURCE["service_id"]
        assert params["generation"] == "generation"
        sql = sql.replace("FROM log_facts FINAL", "FROM log_facts")
        sql = sql.replace("count()", "count(*)")
        sql = sql.replace(
            "toStartOfInterval(timestamp, INTERVAL 1 HOUR, 'UTC')", "time_bucket(INTERVAL '1 hour', timestamp)"
        )
        sql = sql.replace(
            "toStartOfInterval(timestamp, INTERVAL 1 DAY, 'UTC')", "time_bucket(INTERVAL '1 day', timestamp)"
        )
        sql = sql.replace(
            "toStartOfInterval(timestamp, INTERVAL 1 MINUTE, 'UTC')", "time_bucket(INTERVAL '1 minute', timestamp)"
        )
        sql = sql.replace(
            "toStartOfInterval(timestamp, INTERVAL 1 SECOND, 'UTC')", "time_bucket(INTERVAL '1 second', timestamp)"
        )
        for name, kind in (
            ("service", "String"),
            ("generation", "String"),
            ("start", "DateTime64(6, 'UTC')"),
            ("end", "DateTime64(6, 'UTC')"),
        ):
            sql = sql.replace("{" + name + ":" + kind + "}", "$" + name)
        result = self.con.execute(sql, params)
        columns = [c[0] for c in result.description]
        return [dict(zip(columns, row, strict=True)) for row in result.fetchall()]


@pytest.fixture
def facts():
    with duckdb.connect() as con:
        con.execute("SET TimeZone='UTC'")
        con.execute(
            "CREATE TABLE log_facts(service_id VARCHAR, generation VARCHAR, "
            "timestamp TIMESTAMPTZ, country VARCHAR, ip VARCHAR, url VARCHAR, conn_requests BIGINT)"
        )
        rows = [
            (START, "US", "192.0.2.1", "/", 1),
            (END, "GB", "192.0.2.2", "/", 6),
            (START, "", "192.0.2.1", "/", None),
            (START, None, "192.0.2.1", "/", 2),
            (START, "bad-null-ip", None, "/", 1),
            (START, "bad-empty-ip", "", "/", 1),
            (START, "bad-url", "192.0.2.1", None, 1),
            (START, "beacon", "192.0.2.1", "/rum-beacon", 1),
            (START, "beacon-query", "192.0.2.1", "/rum-beacon?x=1", 1),
            (START, "prefix-ok", "192.0.2.1", "/rum-beacon-extra", 1),
            (datetime(2026, 8, 31, 23, 59, 59, 999999, UTC), "before", "192.0.2.1", "/", 1),
            (datetime(2026, 9, 2, 0, 0, 0, 1, UTC), "after", "192.0.2.1", "/", 1),
        ]
        con.executemany(
            "INSERT INTO log_facts VALUES (?, ?, ?, ?, ?, ?, ?)", [(SOURCE["name"], "generation", *r) for r in rows]
        )
        yield con


@pytest.mark.parametrize("interval", ["1 second", "1 minute", "1 hour", "1 day"])
def test_slice_defaults_inclusive_bounds_and_null_country(facts, interval):
    mod = adapter()
    client = FakeClickHouse(facts)
    result = mod.query_slice(client, SOURCE["name"], "generation", START, END, interval)
    assert result["country"]["total"] == 4
    assert {r["value"]: r["count"] for r in result["country"]["top"]} == {"US": 1, "GB": 1, "prefix-ok": 1}
    assert [p["value"] for p in result["time_series"]] == [4.0, 1.0]
    assert datetime.fromisoformat(result["time_series"][-1]["time"].replace("Z", "+00:00")) == END
    assert len(client.calls) == 3


def test_country_total_includes_empty_when_top_is_empty(facts):
    facts.execute("DELETE FROM log_facts WHERE country IS DISTINCT FROM ''")
    result = adapter().query_slice(FakeClickHouse(facts), SOURCE["name"], "generation", START, END, "1 hour")
    assert result["country"] == {"top": [], "total": 1}


def test_empty_slice_has_explicit_zero_totals(facts):
    facts.execute("DELETE FROM log_facts")
    result = adapter().query_slice(FakeClickHouse(facts), SOURCE["name"], "generation", START, END, "1 hour")
    assert result == {"country": {"top": [], "total": 0}, "time_series": []}


def test_top_ties_limit_and_tenant_generation_isolation(facts):
    facts.execute("DELETE FROM log_facts")
    facts.executemany(
        "INSERT INTO log_facts VALUES (?, ?, ?, ?, '192.0.2.1', '/', 1)",
        [(SOURCE["name"], "generation", START, f"country-{n}") for n in range(15)]
        + [(SOURCE["name"], "generation", START, "winner")] * 4
        + [("OtherTenant", "generation", START, "leak")] * 100
        + [(SOURCE["name"], "old-generation", START, "old")] * 100,
    )
    result = adapter().query_slice(FakeClickHouse(facts), SOURCE["name"], "generation", START, END, "1 hour")
    assert result["country"]["total"] == 19
    assert len(result["country"]["top"]) == 10
    assert result["country"]["top"][0] == {"value": "winner", "count": 4}
    assert sum(p["value"] for p in result["time_series"]) == 19


@pytest.mark.parametrize(
    "overrides,reason",
    [
        ({"fields_filter": None}, "unsupported_fields"),
        ({"fields_filter": ["url"]}, "unsupported_fields"),
        ({"sections": {"core"}}, "unsupported_sections"),
        ({"sections": {"core", "topten", "bots"}}, "unsupported_sections"),
        ({"chart_interval": "1 hour; DROP TABLE x"}, "unsupported_interval"),
        ({"chart_metric": "5xx"}, "unsupported_metric"),
        ({"filters": {"country": {"mode": "include", "values": ["US"]}}}, "unsupported_filters"),
        ({"filters": {"country": {"mode": "bad"}}}, "unsupported_filters"),
        ({"start_time": None}, "unbounded_window"),
    ],
)
def test_ineligible_never_opens_clickhouse(facts, monkeypatch, overrides, reason):
    mod = adapter()
    monkeypatch.setattr(mod.config, "load_clickhouse_config", lambda: object())
    monkeypatch.setattr(mod.config, "is_durable_serving_mode", lambda src: True)
    client = Mock(side_effect=AssertionError("must not open ClickHouse"))
    monkeypatch.setattr(mod, "get_clickhouse_client", client)
    baseline = Mock(return_value={"sentinel": True})
    monkeypatch.setattr(mod.dashboard, "get_aggregates", baseline)
    kwargs = dict(
        con=facts,
        src=SOURCE,
        start_time=START.isoformat(),
        end_time=END.isoformat(),
        filters={},
        chart_interval="1 hour",
        chart_metric="requests",
        fields_filter=["country"],
        sections={"core", "topten"},
    )
    kwargs.update(overrides)
    assert mod.get_aggregates(**kwargs)["sentinel"]
    assert not client.called
    assert baseline.call_args.kwargs["_dispatch_reason"] == reason


def test_disabled_no_config_database_or_socket(facts, monkeypatch):
    mod = adapter()
    monkeypatch.setenv("CLICKHOUSE_ENABLED", "false")
    monkeypatch.setattr(mod, "get_clickhouse_client", Mock(side_effect=AssertionError("socket")))
    baseline = Mock(return_value={})
    monkeypatch.setattr(mod.dashboard, "get_aggregates", baseline)
    mod.get_aggregates(
        con=facts,
        src=SOURCE,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 hour",
        chart_metric="requests",
        fields_filter=["country"],
    )
    assert baseline.call_args.kwargs["_dispatch_reason"] == "disabled"


@pytest.fixture
def lake(facts, monkeypatch):
    mod = adapter()
    monkeypatch.setattr(mod.config, "load_clickhouse_config", lambda: object())
    monkeypatch.setattr(mod.config, "is_durable_serving_mode", lambda src: True)
    monkeypatch.setattr(mod.config, "load_config", lambda sid: {})
    monkeypatch.setattr(mod, "source_catalog_identity", lambda: "catalog")
    with duckdb.connect() as con:
        con.execute("LOAD ducklake")
        con.execute("ATTACH 'ducklake::memory:' AS lake (DATA_PATH 'cache/clickhouse-test-unused')")
        con.execute("SET TimeZone='UTC'")
        con.register("fixture", facts.execute("SELECT * FROM log_facts").to_arrow_table())
        con.execute("CREATE TABLE lake.logs_testhybrid AS SELECT timestamp,country,ip,url,conn_requests FROM fixture")
        con.execute("CREATE VIEW logs_TestHybrid AS SELECT * FROM lake.logs_testhybrid")
        monkeypatch.setattr(mod.dashboard, "get_source_extent", lambda *args: (217342, None, None))
        yield con


def eligible_kwargs(con):
    return dict(
        con=con,
        src=SOURCE,
        start_time=START.isoformat(),
        end_time=END.isoformat(),
        filters={},
        chart_interval="1 hour",
        chart_metric="requests",
        fields_filter=["country"],
        sections={"core", "topten"},
    )


def test_hybrid_preserves_full_shape_and_skips_ducklake_selected_queries(lake, facts, monkeypatch):
    from backend.core.clickhouse_publication import Readiness
    from backend.models.dashboard import AggregatesResponse
    from backend.repositories._base import QueryRunner

    mod = adapter()
    kwargs = eligible_kwargs(lake)
    baseline = mod.dashboard.get_aggregates(**{k: v for k, v in kwargs.items() if k != "sections"})
    client = FakeClickHouse(facts)
    monkeypatch.setattr(mod, "get_clickhouse_client", lambda: client)
    ready = Mock(return_value=Readiness(True, "ready", generation="generation"))
    monkeypatch.setattr(mod, "readiness", ready)
    monkeypatch.setattr(QueryRunner, "execute_top_n_batch", Mock(side_effect=AssertionError("duplicate top SQL")))
    original_execute = QueryRunner.execute

    def execute(self, sql, *args, **kw):
        assert "time_bucket(" not in sql, "duplicate time-series SQL"
        return original_execute(self, sql, *args, **kw)

    monkeypatch.setattr(QueryRunner, "execute", execute)
    for _ in range(2):
        result = mod.get_aggregates(**kwargs)
        for key in ("total_rows", "total_rows_total", "metric", "interval"):
            assert result[key] == baseline[key]
        assert sorted(result["map_data"], key=lambda r: r["country"]) == sorted(
            baseline["map_data"], key=lambda r: r["country"]
        )
        assert result["data"]["conn_requests"] == baseline["data"]["conn_requests"]
        assert result["data"]["country"]["total"] == baseline["data"]["country"]["total"]
        assert sorted(result["data"]["country"]["top"], key=lambda r: r["value"]) == sorted(
            baseline["data"]["country"]["top"], key=lambda r: r["value"]
        )
        assert result["time_series"] == baseline["time_series"]
        assert result["total_rows_total"] == 217342
        assert AggregatesResponse(**result).total_rows == 5
    assert ready.call_count == 2
    assert ready.call_args.kwargs["source_snapshot"] == 1
    assert ready.call_args.kwargs["source_table"] == "logs_testhybrid"
    assert len(client.calls) == 6
    assert (
        lake.execute("SELECT count(*) FROM duckdb_views() WHERE view_name LIKE 'logs_ch_request_%'").fetchone()[0] == 0
    )


def test_pinned_view_stays_on_snapshot_and_does_not_replace_original(lake, monkeypatch):
    mod = adapter()
    with mod.pinned_view(lake, SOURCE) as (view, snapshot):
        assert snapshot == 1
        original_count = lake.execute(f"SELECT count(*) FROM {view}").fetchone()[0]
        writer = lake.cursor()
        try:
            writer.execute("INSERT INTO lake.logs_testhybrid VALUES ('2026-09-01', 'new', '192.0.2.1', '/', 1)")
        finally:
            writer.close()
        assert lake.execute(f"SELECT count(*) FROM {view}").fetchone()[0] == original_count
    assert lake.execute("SELECT count(*) FROM logs_TestHybrid").fetchone()[0] == original_count + 1


def test_pinned_view_reuses_field_normalization(lake):
    mod = adapter()
    lake.execute("ALTER TABLE lake.logs_testhybrid ADD COLUMN ttl DOUBLE DEFAULT 1.6")
    lake.execute("ALTER TABLE lake.logs_testhybrid ADD COLUMN timestamp_hour VARCHAR DEFAULT 'wrong'")
    with mod.pinned_view(lake, SOURCE) as (view, _):
        row = lake.execute(f"SELECT ttl, timestamp_hour FROM {view} ORDER BY timestamp LIMIT 1").fetchone()
        assert row == (2, "2026-08-31-23")


@pytest.mark.parametrize(
    "reason", ["outside_coverage", "source_snapshot_mismatch", "target_unverified", "no_active_generation"]
)
def test_readiness_fallback_is_observable_and_uses_original_view(lake, facts, monkeypatch, reason):
    from backend.core.clickhouse_publication import Readiness

    mod = adapter()
    client = FakeClickHouse(facts)
    monkeypatch.setattr(mod, "get_clickhouse_client", lambda: client)
    monkeypatch.setattr(mod, "readiness", lambda *args, **kwargs: Readiness(False, reason))
    result = mod.get_aggregates(**eligible_kwargs(lake))
    assert not client.calls
    assert result["total_rows"] == 5
    assert f"engine:{reason}" in [entry["section"] for entry in result["section_timings"]]


def test_missing_prototype_column_falls_back(lake, monkeypatch):
    mod = adapter()
    lake.execute("DROP VIEW logs_TestHybrid")
    lake.execute("ALTER TABLE lake.logs_testhybrid DROP COLUMN conn_requests")
    lake.execute("CREATE VIEW logs_TestHybrid AS SELECT * FROM lake.logs_testhybrid")
    monkeypatch.setattr(mod, "get_clickhouse_client", Mock(side_effect=AssertionError("no CH needed")))
    result = mod.get_aggregates(**eligible_kwargs(lake))
    assert result["total_rows"] == 5
    assert "engine:unsupported_schema" in [e["section"] for e in result["section_timings"]]


def test_missing_source_table_is_ineligible_not_an_outage(lake, monkeypatch):
    mod = adapter()
    lake.execute("DROP VIEW logs_TestHybrid")
    lake.execute("DROP TABLE lake.logs_testhybrid")
    fallback = Mock(return_value={})
    monkeypatch.setattr(mod.dashboard, "get_aggregates", fallback)
    monkeypatch.setattr(mod, "get_clickhouse_client", Mock(side_effect=AssertionError("no CH needed")))
    assert mod.get_aggregates(**eligible_kwargs(lake)) == {"section_timings": []}
    assert fallback.call_args.kwargs["_dispatch_reason"] == "unsupported_schema"


@pytest.mark.parametrize("at_readiness", [True, False])
def test_outage_never_becomes_zero_or_ducklake_fallback(lake, monkeypatch, at_readiness):
    from backend.core.clickhouse_client import ClickHouseError
    from backend.core.clickhouse_publication import Readiness

    mod = adapter()
    client = Mock()
    error = ClickHouseError("secret upstream response", query_id="test")
    client.execute.side_effect = error
    monkeypatch.setattr(mod, "get_clickhouse_client", lambda: client)
    monkeypatch.setattr(
        mod,
        "readiness",
        Mock(side_effect=error)
        if at_readiness
        else Mock(return_value=Readiness(True, "ready", generation="generation")),
    )
    baseline = Mock(side_effect=AssertionError("no silent fallback"))
    monkeypatch.setattr(mod.dashboard, "get_aggregates", baseline)
    with pytest.raises(mod.HybridUnavailable, match="temporarily unavailable"):
        mod.get_aggregates(**eligible_kwargs(lake))
    baseline.assert_not_called()
    assert lake.execute("SELECT count(*) FROM logs_TestHybrid").fetchone()[0] == 12


def test_target_emptied_after_readiness_cannot_return_success(lake, facts, monkeypatch):
    from backend.core.clickhouse_publication import Readiness

    mod = adapter()
    facts.execute("DELETE FROM log_facts")
    monkeypatch.setattr(mod, "get_clickhouse_client", lambda: FakeClickHouse(facts))
    monkeypatch.setattr(mod, "readiness", lambda *args, **kwargs: Readiness(True, "ready", generation="generation"))
    with pytest.raises(mod.HybridUnavailable):
        mod.get_aggregates(**eligible_kwargs(lake))
