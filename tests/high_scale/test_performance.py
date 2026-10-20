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
