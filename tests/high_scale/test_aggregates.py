from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from backend.high_scale.aggregate_query import query_clickhouse_aggregate
from backend.high_scale.aggregates import AggregateRequest
from backend.high_scale.archive_models import ServingWatermark

NOW = datetime(2026, 9, 11, 12, 0, tzinfo=UTC)


class FakeClient:
    def __init__(self, responses: list[list[dict[str, object]]]) -> None:
        self.responses = list(responses)
        self.sql = ""
        self.params: dict[str, object] | None = None
        self.calls: list[tuple[str, dict[str, object] | None]] = []

    def execute(self, sql: str, params: dict[str, object] | None = None) -> list[dict[str, object]]:
        self.calls.append((sql, params))
        self.sql = sql
        self.params = params
        return self.responses.pop(0)


def watermark(domain: str = "request") -> ServingWatermark:
    return ServingWatermark(
        "svc",
        domain,
        1,
        NOW - timedelta(hours=2),
        NOW - timedelta(minutes=1),
        "cursor",
        "archived",
        "visible",
        True,
    )


def test_query_reads_visible_rows_with_bound_values() -> None:
    client = FakeClient(
        [
            [{"total_count": 9}],
            [
                {"value": "/a", "aggregate_count": 3},
                {"value": "/b", "aggregate_count": 2},
            ],
        ],
    )

    response = query_clickhouse_aggregate(
        client,
        AggregateRequest("svc", "request", NOW - timedelta(hours=1), NOW),
        watermark=watermark(),
        now=NOW,
    )

    assert response.request_count == 9
    assert response.top_values == (("/a", 3), ("/b", 2))
    assert "request_aggregates" in client.sql
    assert "publication_state='visible'" in client.sql
    assert "LIMIT 10" in client.sql
    assert client.params is not None
    assert client.params["service_id"] == "svc"
    assert len(client.calls) == 2
    assert "OVER ()" not in client.calls[1][0]


@pytest.mark.parametrize(
    ("domain", "table", "metric"),
    [
        ("rum_vitals", "rum_vitals_aggregates", "event_count"),
        ("rum_errors", "rum_error_aggregates", "error_count"),
        ("cmcd", "cmcd_aggregates", "event_count"),
    ],
)
def test_domain_tables_are_isolated(domain: str, table: str, metric: str) -> None:
    client = FakeClient([[], []])

    query_clickhouse_aggregate(
        client,
        AggregateRequest("svc", domain),
        watermark=watermark(domain),
        now=NOW,
    )

    assert table in client.sql
    assert f"sum({metric})" in client.sql


def test_range_parameters_use_a_clickhouse_type_matching_the_parameter_format() -> None:
    """Regression test for a real bug found live: ClickHouseClient._parameter()
    always formats a datetime with microsecond precision (matching every
    other DateTime64(3) parameter in this codebase — see query_service.py),
    but this query annotated its range parameters as plain {..:DateTime}.
    ClickHouse's DateTime parser rejects a fractional-seconds suffix, so
    every range-bounded call failed with a live HTTP 500 (empty request
    windows never hit the range clause, so no fake-client unit test caught
    it)."""
    client = FakeClient([[], []])

    query_clickhouse_aggregate(
        client,
        AggregateRequest("svc", "request", NOW - timedelta(hours=1), NOW),
        watermark=watermark(),
        now=NOW,
    )

    assert "{start:DateTime64(3)}" in client.sql
    assert "{end:DateTime64(3)}" in client.sql
    assert "bucket_start <= {end:DateTime64(3)}" in client.sql
    assert "{start:DateTime}" not in client.sql
    assert "{end:DateTime}" not in client.sql


def test_query_allows_digit_suffix_in_internal_dimension_identifier() -> None:
    client = FakeClient([[], []])

    query_clickhouse_aggregate(
        client,
        AggregateRequest("svc", "request", dimension="is_ipv6"),
        watermark=watermark(),
        now=NOW,
    )

    assert client.params is not None
    assert client.params["dimension"] == "is_ipv6"


@pytest.mark.parametrize("dimension", ["3xx", "ja-3", "url; DROP TABLE request_aggregates"])
def test_query_rejects_non_identifier_dimension(dimension: str) -> None:
    with pytest.raises(ValueError, match="internal identifier"):
        query_clickhouse_aggregate(
            FakeClient([[], []]),
            AggregateRequest("svc", "request", dimension=dimension),
            watermark=watermark(),
            now=NOW,
        )


def test_watermark_must_match_request_domain() -> None:
    with pytest.raises(ValueError, match="watermark"):
        query_clickhouse_aggregate(
            FakeClient([[], []]),
            AggregateRequest("svc", "request"),
            watermark=watermark("cmcd"),
        )


# ── Merged from test_aggregate_writer.py ──

from datetime import UTC, datetime

import pytest

from backend.high_scale.aggregate_writer import compute_dimension_counts


def test_computes_counts_per_dimension_value_and_minute_bucket() -> None:
    rows = (
        {"timestamp": "2026-09-14T00:00:10Z", "url": "/a", "country": "US", "ip": "1.1.1.1"},
        {"timestamp": "2026-09-14T00:00:40Z", "url": "/a", "country": "US", "ip": "1.1.1.2"},
        {"timestamp": "2026-09-14T00:01:05Z", "url": "/b", "country": "CA", "ip": "1.1.1.1"},
    )

    counts = compute_dimension_counts("request", rows)

    by_key = {(row["dimension"], row["value"], row["bucket_start"]): row["count"] for row in counts}
    minute0 = datetime(2026, 9, 14, 0, 0, tzinfo=UTC).isoformat()
    minute1 = datetime(2026, 9, 14, 0, 1, tzinfo=UTC).isoformat()
    assert by_key[("url", "/a", minute0)] == 2
    assert by_key[("url", "/b", minute1)] == 1
    assert by_key[("country", "US", minute0)] == 2
    assert by_key[("country", "CA", minute1)] == 1
    assert by_key[("client_ip", "1.1.1.1", minute0)] == 1
    assert by_key[("client_ip", "1.1.1.2", minute0)] == 1


def test_request_counts_include_all_dashboard_card_dimensions() -> None:
    rows = (
        {
            "timestamp": "2026-09-14T00:00:10Z",
            "url": "/a",
            "country": "US",
            "ip": "1.1.1.1",
            "host": "logs.example.test",
            "method": "GET",
            "status": 200,
            "cache": "HIT",
            "proto": "HTTP/3",
            "ua": "Synthetic Browser",
            "referer": "https://example.test/",
            "city": "Seattle",
            "region": "WA",
            "pop": "SEA",
            "backend": "origin-a",
            "tls": "1.3",
        },
    )

    counts = compute_dimension_counts("request", rows)

    values = {(row["dimension"], row["value"]) for row in counts}
    assert {
        ("host", "logs.example.test"),
        ("method", "GET"),
        ("status", "200"),
        ("cache", "HIT"),
        ("proto", "HTTP/3"),
        ("ua", "Synthetic Browser"),
        ("referer", "https://example.test/"),
        ("city", "Seattle"),
        ("region", "WA"),
        ("pop", "SEA"),
        ("backend", "origin-a"),
        ("tls", "1.3"),
    } <= values


def test_rum_vitals_uses_metric_name_dimension() -> None:
    rows = ({"timestamp": "2026-09-14T00:00:00Z", "metric_name": "LCP"},)

    counts = compute_dimension_counts("rum_vitals", rows)

    assert len(counts) == 1
    assert counts[0]["dimension"] == "metric_name"
    assert counts[0]["value"] == "LCP"


def test_cmcd_dimension_reads_the_session_id_field() -> None:
    rows = ({"timestamp": "2026-09-14T00:00:00Z", "sid": "session-1"},)

    counts = compute_dimension_counts("cmcd", rows)

    assert counts[0]["dimension"] == "cmcd_session"
    assert counts[0]["value"] == "session-1"


def test_rows_without_a_timestamp_are_skipped() -> None:
    counts = compute_dimension_counts("request", ({"url": "/a"},))

    assert counts == ()


def test_unsupported_domain_is_rejected() -> None:
    with pytest.raises(ValueError, match="domain"):
        compute_dimension_counts("bogus", ())


# ── Merged from test_projections.py ──

import pytest

from backend.high_scale.projections import project_cmcd


def test_cmcd_projection_preserves_request_identity_and_full_session_id() -> None:
    request = {
        "event_id": "request-event-1",
        "service_id": "svc",
        "timestamp": "2026-09-01T00:00:00Z",
        "cmcd_version": "v2",
        "sid": "session-" + "x" * 64,
        "cmcd": {"br": 1200, "d": 4000},
    }

    projection = project_cmcd(request)

    assert projection is not None
    assert projection["request_event_id"] == "request-event-1"
    assert projection["projection_key"]
    assert projection["sid"] == request["sid"]
    assert projection["cmcd_version"] == "v2"
    assert projection["fields"] == {"br": 1200, "d": 4000}


def test_cmcd_projection_is_absent_when_request_has_no_cmcd() -> None:
    assert project_cmcd({"event_id": "request-event-1", "service_id": "svc"}) is None


def test_cmcd_projection_requires_request_event_identity() -> None:
    with pytest.raises(ValueError, match="request_event_id"):
        project_cmcd({"service_id": "svc", "cmcd": {"br": 1200}})


def test_cmcd_projection_carries_the_request_geography_the_analytics_group_by() -> None:
    request = {
        "event_id": "request-event-1",
        "service_id": "svc",
        "timestamp": "2026-09-01T00:00:00Z",
        "country": "US",
        "asn": 7922,
        "cmcd": {"br": "1200"},
    }

    projection = project_cmcd(request)

    assert projection is not None
    assert projection["country"] == "US"
    assert projection["asn"] == 7922


@pytest.mark.parametrize(
    ("raw_asn", "expected"), [("15169", 15169), ("", None), ("AS15169", None), (None, None), (-4, None)]
)
def test_cmcd_projection_normalises_asn_or_drops_it(raw_asn, expected) -> None:
    projection = project_cmcd(
        {"event_id": "e", "service_id": "svc", "timestamp": "2026-09-01T00:00:00Z", "asn": raw_asn, "cmcd": {"br": "1"}}
    )

    assert projection is not None
    assert projection["asn"] == expected


def test_cmcd_projection_defaults_missing_country_to_empty() -> None:
    projection = project_cmcd(
        {"event_id": "e", "service_id": "svc", "timestamp": "2026-09-01T00:00:00Z", "cmcd": {"br": "1"}}
    )

    assert projection is not None
    assert projection["country"] == ""


# ── Merged from test_projection_utils.py ──

from datetime import UTC, datetime

import pytest

from backend.high_scale._projection_utils import bucket, finite_float, percentiles, range_value, text


def test_range_value_keeps_naive_for_date_only_and_tz_for_z():
    assert range_value(None) is None
    assert range_value("2026-01-02T03:04:05Z") == datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    assert range_value("2026-01-02").tzinfo is None


def test_bucket_floors_to_utc_minute_and_rejects_garbage():
    assert bucket("2026-01-02T03:04:59.9Z") == datetime(2026, 1, 2, 3, 4, tzinfo=UTC)
    assert bucket(datetime(2026, 1, 2, 3, 4, 59, 5, tzinfo=UTC)) == datetime(2026, 1, 2, 3, 4, tzinfo=UTC)
    assert bucket("not a date") is None
    assert bucket(5) is None


def test_text_and_finite_float():
    assert text("  x ") == "x"
    assert text(5) == ""
    assert finite_float("1.5") == 1.5
    assert finite_float(True) is None
    assert finite_float("") is None
    assert finite_float("nan") is None
    assert finite_float("inf") is None


@pytest.mark.parametrize(
    "values,q,expected",
    [([], 0.5, None), ([4.0], 0.99, 4.0), ([1.0, 2.0, 3.0, 4.0], 0.5, 2.5), ([10.0, 0.0], 0.95, pytest.approx(9.5))],
)
def test_percentiles_linear_interpolation(values, q, expected):
    assert percentiles(values, (q,)) == [expected]


def test_percentiles_sorts_once(monkeypatch):
    import builtins

    from backend.high_scale import _projection_utils as utils

    calls = []
    real_sorted = builtins.sorted
    monkeypatch.setattr(utils, "sorted", lambda *a, **k: calls.append(1) or real_sorted(*a, **k), raising=False)
    utils.percentiles([3.0, 1.0, 2.0], (0.5, 0.95, 0.99))
    assert len(calls) == 1


# ── Merged from test_iso_parse_pins.py ──

from datetime import UTC, datetime

import pytest

from backend.high_scale import publication, raw_query, sessions


@pytest.mark.parametrize("module", [sessions])
def test_parse_ts_range_value_family(module) -> None:
    assert module._parse_ts(None) is None
    assert module._parse_ts("") is None
    assert module._parse_ts("2026-01-02").tzinfo is None
    assert module._parse_ts("2026-01-02T03:04:05Z") == datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    assert module._parse_ts("2026-01-02T03:04:05+02:00").utcoffset().total_seconds() == 7200


def test_sessions_parse_ts_raises_on_garbage() -> None:
    with pytest.raises(ValueError):
        sessions._parse_ts("not-a-date")


def test_publication_timestamp_assumes_utc_for_naive_and_wraps_errors() -> None:
    assert publication._timestamp({"t": "2026-01-02"}, "t") == datetime(2026, 1, 2, tzinfo=UTC)
    assert publication._timestamp({"t": "2026-01-02T03:04:05Z"}, "t") == datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    with pytest.raises(ValueError, match="t must be an ISO timestamp"):
        publication._timestamp({"t": "garbage"}, "t")


@pytest.mark.parametrize(
    "fn,key",
    [(raw_query._row_timestamp, "timestamp")],
)
def test_astimezone_family_returns_utc_aware(fn, key) -> None:
    assert fn({key: "2026-01-02T03:04:05Z"}) == datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    assert fn({key: "2026-01-02T03:04:05+02:00"}) == datetime(2026, 1, 2, 1, 4, 5, tzinfo=UTC)
    assert fn({key: "2026-01-02"}).tzinfo is not None
    assert fn({key: 5}) is None
    with pytest.raises(ValueError):
        fn({key: "garbage"})


# ── Merged from test_sketch.py ──

import math
import random

import pytest

from backend.high_scale._sketch import RELATIVE_ERROR, sketch


def _weighted_quantile(centroids: list[float], counts: list[int], q: float) -> float:
    """Mirror of ClickHouse ``quantileExactWeighted``: first centroid whose cumulative weight reaches ``q``."""
    target = q * sum(counts)
    seen = 0
    for centroid, count in zip(centroids, counts, strict=True):
        seen += count
        if seen >= target:
            return centroid
    return centroids[-1]


def test_empty_input_yields_empty_sketch() -> None:
    assert sketch([]) == ([], [])


def test_counts_conserve_every_observation() -> None:
    values = [0.0, 0.0, 5.0, 5.0, 5.1, 1_000.0, -20.0]

    centroids, counts = sketch(values)

    assert sum(counts) == len(values)
    assert centroids == sorted(centroids)
    assert len(centroids) == len(counts)


def test_zero_and_negative_values_keep_their_sign() -> None:
    centroids, counts = sketch([0.0, -100.0, 100.0])

    assert centroids[0] < 0 < centroids[2]
    assert centroids[1] == 0.0
    assert counts == [1, 1, 1]


def test_nearby_values_share_a_centroid_and_far_values_do_not() -> None:
    centroids, counts = sketch([1000.0, 1000.5, 50_000.0])

    assert counts == [2, 1]
    assert len(centroids) == 2


@pytest.mark.parametrize("q", [0.5, 0.75, 0.95, 0.99])
def test_merged_quantiles_stay_within_the_documented_relative_error(q: float) -> None:
    rng = random.Random(7)
    minutes = [[rng.lognormvariate(8.0, 1.2) for _ in range(300)] for _ in range(20)]
    merged: dict[float, int] = {}
    for minute in minutes:
        centroids, counts = sketch(minute)
        for centroid, count in zip(centroids, counts, strict=True):
            merged[centroid] = merged.get(centroid, 0) + count
    everything = sorted(value for minute in minutes for value in minute)
    exact = everything[math.ceil(q * len(everything)) - 1]

    estimate = _weighted_quantile(sorted(merged), [merged[c] for c in sorted(merged)], q)

    assert estimate == pytest.approx(exact, rel=RELATIVE_ERROR * 1.001)
