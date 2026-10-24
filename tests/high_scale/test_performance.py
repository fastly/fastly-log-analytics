from __future__ import annotations

from types import SimpleNamespace

import pytest

from backend.high_scale import performance as hs_performance
from backend.models.common import FilterSpec
from backend.models.performance import PerformanceAggregatesResponse

START = "2026-09-16T14:00:00Z"
END = "2026-09-16T15:00:00Z"

_PERCENTILE_ROW = {
    "value": "/slow",
    "req_count": 9,
    "avg_ms": 12.5,
    "p50_ms": 10.0,
    "p95_ms": 20.0,
    "p99_ms": 30.0,
}


class _Client:
    """Records every statement; answers by the ``/* perf:<marker> */`` comment."""

    def __init__(self, answers: dict[str, list[dict]] | None = None) -> None:
        self.answers = answers or {}
        self.calls: list[tuple[str, dict]] = []

    def execute(self, sql, params=None):
        self.calls.append((sql, params or {}))
        marker = sql.split("/* perf:", 1)[1].split(" */", 1)[0]
        return self.answers.get(marker, [])

    def sql_for(self, marker: str) -> tuple[str, dict]:
        return next(call for call in self.calls if f"/* perf:{marker} */" in call[0])

    @property
    def markers(self) -> set[str]:
        return {sql.split("/* perf:", 1)[1].split(" */", 1)[0] for sql, _ in self.calls}


def _service(client: _Client) -> SimpleNamespace:
    return SimpleNamespace(service_id="svc", client=client)


def _req(**kwargs) -> SimpleNamespace:
    return SimpleNamespace(**{"filters": {}, "sort_by": "p99", **kwargs})


@pytest.fixture(autouse=True)
def _asn_names(monkeypatch):
    monkeypatch.setattr(
        "backend.core.duckdb.get_asn_names", lambda service_id, asns: {7922: "Comcast Cable"} if asns else {}
    )


def _run(client: _Client, req=None, sections=None, start=START, end=END):
    return hs_performance.performance_aggregates(_service(client), req or _req(), start, end, sections=sections)


def test_waterfall_is_the_summary_weighted_average_not_an_empty_stub():
    client = _Client(
        {
            "waterfall": [
                {
                    "component_count": 4,
                    "edge_processing": 12.5,
                    "origin_wait": 40.0,
                    "origin_download": 5.0,
                    "client_download": 9.25,
                }
            ]
        }
    )

    res = _run(client, sections={"waterfall", "scatter"})

    assert res.waterfall.avg is not None
    assert res.waterfall.avg.model_dump() == {
        "edge_processing": 12.5,
        "origin_wait": 40.0,
        "origin_download": 5.0,
        "client_download": 9.25,
    }
    sql, params = client.sql_for("waterfall")
    assert "FROM performance_minute_summary" in sql
    assert "sum(edge_processing_sum_ms) / nullIf(sum(component_count), 0)" in sql
    assert "publication_state='visible'" in sql
    assert params["service_id"] == "svc"


def test_unfiltered_waterfall_does_not_alias_an_aggregate_over_the_column_it_divides_by():
    # ClickHouse resolves `sum(c) AS c` then `sum(c)` to the alias and rejects the
    # nested aggregate (HTTP 500). Fakes cannot catch it, so assert the SQL shape.
    client = _Client({"waterfall": [{"n_components": 1, "edge_processing": 1.0}]})

    _run(client, sections={"waterfall"})

    sql, _ = client.sql_for("waterfall")
    assert "AS component_count" not in sql


def test_waterfall_with_no_rows_reports_zero_averages_like_the_standard_path():
    res = _run(
        _Client(
            {
                "waterfall": [
                    {
                        "component_count": 0,
                        "edge_processing": None,
                        "origin_wait": None,
                        "origin_download": None,
                        "client_download": None,
                    }
                ]
            }
        ),
        sections={"waterfall"},
    )

    assert res.waterfall.avg is not None
    assert res.waterfall.avg.model_dump() == dict.fromkeys(
        ("edge_processing", "origin_wait", "origin_download", "client_download"), 0.0
    )


def test_scatter_is_the_uniform_300_row_sample_of_hit_and_miss_points():
    client = _Client(
        {
            "scatter": [
                {"origin_ms": 100.0, "edge_ms": 150.0, "cache": "MISS"},
                {"origin_ms": 20.0, "edge_ms": 100.0, "cache": "HIT"},
                {"origin_ms": 5.0, "edge_ms": 1.0, "cache": ""},
            ]
        }
    )

    res = _run(client, sections={"scatter", "waterfall"})

    assert [(p.origin, p.edge, p.cache) for p in res.scatter] == [
        (100.0, 150.0, "MISS"),
        (20.0, 100.0, "HIT"),
        (5.0, 1.0, None),
    ]
    sql, params = client.sql_for("scatter")
    assert "FROM performance_scatter_samples" in sql
    assert "ORDER BY sample_key" in sql
    assert params["scatter_limit"] == 300


def test_top_asns_carry_resolved_labels_and_ints():
    row = {**_PERCENTILE_ROW, "value": "7922", "req_count": 40}
    res = _run(_Client({"top_asns": [row, {**row, "value": "64512"}]}), sections={"top_asns", "top_urls"})

    assert [a.asn for a in res.top_asns] == [7922, 64512]
    assert res.top_asns[0].label == "Comcast Cable (7922)"
    assert res.top_asns[1].label == "AS64512"


def test_projection_latencies_are_converted_from_stored_microseconds_to_milliseconds():
    client = _Client({"top_urls": [_PERCENTILE_ROW]})

    res = _run(client, sections={"top_urls", "top_asns"})

    url = res.top_urls[0]
    assert (url.avg, url.p50, url.p95, url.p99) == (12.5, 10.0, 20.0, 30.0)
    sql, _ = client.sql_for("top_urls")
    assert sql.count("/ 1000.0") == 4


@pytest.mark.parametrize(
    ("sort_by", "column"), [("avg", "avg_ms"), ("p50", "p50_ms"), ("p95", "p95_ms"), ("p99", "p99_ms")]
)
def test_sort_by_orders_the_top_tables(sort_by, column):
    client = _Client()

    _run(client, _req(sort_by=sort_by), sections={"top_urls", "top_asns"})

    for marker in ("top_urls", "top_asns"):
        sql, _ = client.sql_for(marker)
        assert f"ORDER BY {column} DESC NULLS LAST" in sql


def test_top_asns_require_more_than_ten_requests_like_the_standard_path():
    client = _Client()

    _run(client, sections={"top_urls", "top_asns"})

    assert "HAVING req_count > 5" in client.sql_for("top_urls")[0]
    assert "HAVING req_count > 10" in client.sql_for("top_asns")[0]


def test_sections_prune_queries_and_pairs_are_expected_from_the_router():
    client = _Client()

    res = _run(client, sections={"ttl_dist"})

    assert client.markers == {"ttl_dist"}
    assert res.top_urls == [] and res.top_asns == [] and res.scatter == []
    assert res.waterfall.avg is None


def test_none_sections_serve_every_section():
    client = _Client()

    _run(client, sections=None)

    assert client.markers == {"top_urls", "top_asns", "ttl_dist", "waterfall", "scatter"}


def test_ttl_dist_uses_every_ttl_value_and_the_standard_bucket_edges():
    client = _Client(
        {
            "ttl_dist": [
                {"bucket": "0s", "count": 3, "min_ttl": 0},
                {"bucket": "<30m", "count": 7, "min_ttl": 1201},
            ]
        }
    )

    res = _run(client, sections={"ttl_dist"})

    assert [(b.bucket, b.count) for b in res.ttl_dist] == [("0s", 3), ("<30m", 7)]
    sql, _ = client.sql_for("ttl_dist")
    assert "FROM request_aggregates" in sql
    assert "LIMIT" not in sql
    for edge in ("'<30m'", "'<3h'", "'<1w'", "'<1y'", "'>1y'"):
        assert edge in sql


def test_unfiltered_requests_read_only_projection_tables():
    client = _Client()

    _run(client)

    assert not any("request_facts" in sql for sql, _ in client.calls)
    for sql, params in client.calls:
        assert "high_scale_batch_publications" in sql
        assert params["start"].isoformat().startswith("2026-09-16T14:00:00")


def test_filters_are_applied_against_request_facts_for_every_section():
    client = _Client(
        {
            "top_urls": [_PERCENTILE_ROW],
            "waterfall": [
                {
                    "component_count": 1,
                    "edge_processing": 1.0,
                    "origin_wait": 2.0,
                    "origin_download": 3.0,
                    "client_download": 4.0,
                }
            ],
            "scatter": [{"origin_ms": 1.0, "edge_ms": 2.0, "cache": "HIT"}],
        }
    )
    req = _req(filters={"country": FilterSpec(mode="include", values=["US"])})

    res = _run(client, req)

    assert res.top_urls[0].p99 == 30.0
    assert res.waterfall.avg is not None and res.waterfall.avg.client_download == 4.0
    assert res.scatter[0].cache == "HIT"
    assert res.approx is False
    assert client.markers == {"top_urls", "top_asns", "ttl_dist", "waterfall", "scatter"}
    for sql, params in client.calls:
        assert "FROM request_facts" in sql
        assert "performance_minute" not in sql and "performance_scatter" not in sql
        assert "country IN ({filter_0_0:String})" in sql
        assert params["filter_0_0"] == "US"
        assert "publication_state='visible'" in sql
        assert "event_timestamp >=" in sql
    assert "ORDER BY cityHash64(event_id)" in client.sql_for("scatter")[0]


def test_filtered_sort_by_is_honored():
    client = _Client()

    _run(
        client,
        _req(filters={"asn": FilterSpec(mode="exclude", values=["64512"])}, sort_by="avg"),
        sections={"top_urls", "top_asns"},
    )

    sql, _ = client.sql_for("top_urls")
    assert "ORDER BY avg_ms DESC NULLS LAST" in sql
    assert "NOT IN" in client.sql_for("top_asns")[0]


def test_filter_on_a_field_without_a_facts_mapping_is_rejected_not_ignored():
    with pytest.raises(ValueError, match="unsupported"):
        _run(_Client(), _req(filters={"not_a_field": FilterSpec(mode="include", values=["x"])}))


def test_empty_filter_values_do_not_force_the_filtered_path():
    client = _Client()

    _run(client, _req(filters={"country": FilterSpec(mode="include", values=[])}), sections={"waterfall"})

    assert "FROM performance_minute_summary" in client.sql_for("waterfall")[0]


def test_response_is_marked_approximate_and_validates_against_the_wire_model():
    res = _run(_Client({"top_urls": [_PERCENTILE_ROW]}))

    assert isinstance(res, PerformanceAggregatesResponse)
    assert res.approx is True


# ── Merged from test_performance_projection.py ──

from copy import deepcopy

from backend.high_scale.performance_projection import (
    SCATTER_SAMPLE_SIZE,
    build_performance_projection_rows,
)


def _event(**overrides):
    event = {
        "event_id": "evt-1",
        "timestamp": "2026-09-16T14:00:01Z",
        "url": "/a",
        "asn": "7922",
        "cache": "MISS",
        "elapsed": 250_000,
        "ttfb": 0.1,
        "ottfb": 40_000,
        "ottlb": 90_000,
    }
    event.update(overrides)
    return event


def test_summary_rows_decompose_latency_into_waterfall_components_in_ms():
    events = (
        _event(),
        _event(event_id="evt-2", timestamp="2026-09-16T14:00:30Z", elapsed=150_000, ttfb=0.05, ottfb=None, ottlb=None),
        _event(event_id="evt-3", timestamp="2026-09-16T14:01:00Z"),
    )
    original = deepcopy(events)

    projection = build_performance_projection_rows(events)

    assert events == original
    assert projection.summary_rows == (
        {
            "bucket_start": "2026-09-16T14:00:00+00:00",
            "component_count": 2,
            # evt-1: 60 edge, 40 wait, 50 download, 150 client; evt-2: 50 edge, 0, 0, 100 client
            "edge_processing_sum_ms": 110.0,
            "origin_wait_sum_ms": 40.0,
            "origin_download_sum_ms": 50.0,
            "client_download_sum_ms": 250.0,
        },
        {
            "bucket_start": "2026-09-16T14:01:00+00:00",
            "component_count": 1,
            "edge_processing_sum_ms": 60.0,
            "origin_wait_sum_ms": 40.0,
            "origin_download_sum_ms": 50.0,
            "client_download_sum_ms": 150.0,
        },
    )


def test_components_are_clamped_at_zero_like_the_standard_path():
    # ottlb < ottfb and ttfb < ottfb: every derived component floors at 0.
    projection = build_performance_projection_rows((_event(elapsed=10_000, ttfb=0.001, ottfb=40_000, ottlb=20_000),))

    row = projection.summary_rows[0]
    assert row["edge_processing_sum_ms"] == 0.0
    assert row["origin_download_sum_ms"] == 0.0
    assert row["client_download_sum_ms"] == 0.0
    assert row["origin_wait_sum_ms"] == 40.0


def test_rows_without_ttfb_or_elapsed_never_enter_summary_or_scatter():
    projection = build_performance_projection_rows(
        (_event(ttfb=None), _event(event_id="e2", elapsed=""), _event(event_id="e3", ttfb="nan"))
    )

    assert projection.summary_rows == ()
    assert projection.scatter_rows == ()


def test_scatter_rows_carry_origin_edge_and_cache_and_drop_negative_edge():
    projection = build_performance_projection_rows(
        (
            _event(),
            _event(event_id="evt-hit", cache="HIT", elapsed=120_000, ttfb=0.02),
            _event(event_id="evt-neg", elapsed=1_000, ttfb=0.5),
            _event(event_id="evt-nocache", cache=None),
        )
    )

    shapes = {(row["cache"], row["origin_ms"], row["edge_ms"]) for row in projection.scatter_rows}
    assert shapes == {("MISS", 100.0, 150.0), ("HIT", 20.0, 100.0), ("", 100.0, 150.0)}
    assert all(0 <= row["sample_key"] < 2**64 for row in projection.scatter_rows)
    assert all(row["bucket_start"] == "2026-09-16T14:00:00+00:00" for row in projection.scatter_rows)


def test_scatter_keeps_the_lowest_sample_keys_so_merged_batches_stay_uniform():
    events = tuple(_event(event_id=f"evt-{i}") for i in range(SCATTER_SAMPLE_SIZE * 2))

    projection = build_performance_projection_rows(events)

    assert len(projection.scatter_rows) == SCATTER_SAMPLE_SIZE
    keys = [row["sample_key"] for row in projection.scatter_rows]
    assert keys == sorted(keys)
    again = build_performance_projection_rows(tuple(reversed(events)))
    assert [row["sample_key"] for row in again.scatter_rows] == keys


def test_dimension_rows_are_unchanged():
    projection = build_performance_projection_rows((_event(), _event(event_id="e2", url="/b")))

    assert {(r["dimension"], r["value"]) for r in projection.dimension_rows} == {
        ("url", "/a"),
        ("url", "/b"),
        ("asn", "7922"),
    }


def test_integer_asn_from_the_vcl_log_format_still_produces_an_asn_dimension():
    # The log format emits asn unquoted, so real events carry an int, not "7922".
    projection = build_performance_projection_rows((_event(asn=7922), _event(event_id="evt-2", asn=15169)))

    asn_values = {row["value"] for row in projection.dimension_rows if row["dimension"] == "asn"}

    assert asn_values == {"7922", "15169"}


# ── Merged from test_backfill_performance_script.py ──

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from backend.high_scale.recovery import RecoveryReport


@pytest.fixture
def script(monkeypatch: pytest.MonkeyPatch):
    from scripts import backfill_high_scale as module

    service = SimpleNamespace(manifest_catalog=object(), archive=object(), client=MagicMock())
    registry = MagicMock()
    registry.resolve.return_value = service
    monkeypatch.setattr(module.app_config, "load_clickhouse_config", lambda: object())
    monkeypatch.setattr(module, "ClickHouseClient", lambda settings: MagicMock())
    monkeypatch.setattr(module, "get_high_scale_service_registry", lambda: registry)
    monkeypatch.setattr(module, "register_high_scale_services_from_environment", lambda *a, **k: None)
    monkeypatch.setattr(module, "PostgresControlPlane", MagicMock)
    monkeypatch.setattr(module, "PostgresBatchManifestStore", lambda control: MagicMock())
    monkeypatch.setattr(module, "AggregateBatchAdapter", lambda client: MagicMock())
    monkeypatch.setattr(module, "ClickHousePublication", lambda manifests, adapter: MagicMock())
    return module


def _run_backfill_script(script, monkeypatch, argv: list[str], *, complete: bool = True):
    seen: dict = {}

    def fake_rebuild(*args, **kwargs):
        seen.update(kwargs)
        return RecoveryReport(True, complete, True, complete, complete, 2, 5, (), "rb-1")

    monkeypatch.setattr(script, "rebuild_performance_projections_from_fos", fake_rebuild)
    monkeypatch.setattr("sys.argv", ["backfill", *argv])
    return script.main_performance(), seen


def test_until_caps_the_window_at_the_worker_deploy_time(script, monkeypatch, capsys) -> None:
    code, seen = _run_backfill_script(
        script, monkeypatch, ["--service-id", "svc", "--until", "2026-10-20T12:00:00Z", "--days", "7"]
    )

    assert code == 0
    assert seen["now"] == datetime(2026, 10, 20, 12, 0, tzinfo=UTC)
    assert seen["window"] == timedelta(days=7)
    assert seen["service_id"] == "svc"
    assert '"rebuild_id": "rb-1"' in capsys.readouterr().out


def test_incomplete_rebuild_exits_nonzero(script, monkeypatch) -> None:
    assert (
        _run_backfill_script(
            script, monkeypatch, ["--service-id", "svc", "--until", "2026-10-20T12:00:00Z"], complete=False
        )[0]
        == 1
    )


def test_until_is_required_and_must_carry_an_offset(script, monkeypatch) -> None:
    with pytest.raises(SystemExit) as missing:
        _run_backfill_script(script, monkeypatch, ["--service-id", "svc"])
    with pytest.raises(SystemExit) as naive:
        _run_backfill_script(script, monkeypatch, ["--service-id", "svc", "--until", "2026-10-20T12:00:00"])

    assert missing.value.code == 2
    assert naive.value.code == 2


def test_rebuild_id_is_generated_and_printed_before_any_work_so_a_killed_run_can_resume(
    script, monkeypatch, capsys
) -> None:
    _, seen = _run_backfill_script(script, monkeypatch, ["--service-id", "svc", "--until", "2026-10-20T12:00:00Z"])

    assert seen["rebuild_id"], "the script must pin a rebuild id itself instead of letting the rebuild pick one"
    assert f"rebuild_id={seen['rebuild_id']}" in capsys.readouterr().err


def test_an_explicit_rebuild_id_is_passed_through_unchanged(script, monkeypatch) -> None:
    _, seen = _run_backfill_script(
        script, monkeypatch, ["--service-id", "svc", "--until", "2026-10-20T12:00:00Z", "--rebuild-id", "abc123"]
    )

    assert seen["rebuild_id"] == "abc123"
