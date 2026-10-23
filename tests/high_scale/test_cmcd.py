"""High-scale CMCD analytics computed from ``cmcd_projection_facts`` (no hardcoded empty sections)."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from backend.core.high_scale_sections import current_clickhouse_scope
from backend.high_scale import cmcd as hs_cmcd
from backend.high_scale.cmcd import cmcd_aggregates
from backend.models.cmcd import CmcdAggregatesResponse, CmcdRequest

START = "2026-09-14T10:00:00Z"
END = "2026-09-14T10:15:00Z"
ALL_SECTIONS = {
    "overview",
    "sessions_ts",
    "buffer_health_ts",
    "bitrate_ts",
    "throughput_ts",
    "top_content",
    "rebuffer_by_country",
    "rebuffer_by_asn",
    "object_type_dist",
    "streaming_format_dist",
    "startup_ts",
    "session_duration_dist",
}
_PRESENT = {
    "events": 40,
    "sessions": 4,
    "has_bl": 1,
    "has_br": 1,
    "has_bs": 1,
    "has_mtp": 1,
    "has_ot": 1,
    "has_sf": 1,
    "has_su": 1,
    "has_tb": 1,
    "has_cid": 1,
    "has_country": 1,
    "has_asn": 1,
}


class _Client:
    def __init__(self, probe: dict[str, Any] | None = None) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.scopes: list[dict] = []
        self._probe = probe if probe is not None else _PRESENT

    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        self.calls.append((sql, params or {}))
        self.scopes.append(current_clickhouse_scope())
        if "cmcd:probe" in sql:
            return [self._probe]
        if "cmcd:overview_peak" in sql:
            return [{"peak_viewers": 4}]
        if "cmcd:overview_duration" in sql:
            return [{"avg_duration": 125.4}]
        if "cmcd:overview" in sql:
            return [
                {
                    "active_sessions": 4,
                    "rebuffer_rate": 25.0,
                    "avg_bitrate": 2500.126,
                    "avg_buffer_length": 12.0,
                    "median_throughput": 8000.0,
                }
            ]
        if "cmcd:timeseries" in sql:
            return [
                {
                    "bucket": "2026-09-14 10:05:00",
                    "concurrent_sessions": 3,
                    "rebuffer_session_pct": 33.333,
                    "p50_buffer": 10.0,
                    "p95_buffer": 20.0,
                    "starvation_rate": 5.0,
                    "avg_bitrate": 2400.0,
                    "utilization_ratio": 0.51234,
                    "p50_throughput": 7000.0,
                    "p95_throughput": 9000.0,
                    "p99_throughput": 9500.0,
                    "startup_ratio": 2.5,
                }
            ]
        if "cmcd:new_sessions" in sql:
            return [{"bucket": "2026-09-14 10:05:00", "new_sessions": 2}]
        if "cmcd:top_content" in sql:
            return [
                {"content_id": "movie-1", "session_count": 3, "rebuffer_rate": 33.33, "avg_br": 3000.0, "avg_bl": 9.5}
            ]
        if "cmcd:rebuffer_by_country" in sql:
            return [{"country": "US", "rebuffer_rate": 10.0, "session_count": 3, "median_mtp": 7500.0}]
        if "cmcd:rebuffer_by_asn" in sql:
            return [{"asn": 7922, "rebuffer_rate": 50.0, "session_count": 2, "avg_br": 2000.0, "avg_bl": 8.0}]
        if "cmcd:object_type_dist" in sql:
            return [{"object_type": "v", "request_count": 30}, {"object_type": "a", "request_count": 10}]
        if "cmcd:streaming_format_dist" in sql:
            return [{"streaming_format": "h", "session_count": 4}]
        if "cmcd:session_duration_dist" in sql:
            return [{"sort_order": 2, "session_count": 1}, {"sort_order": 5, "session_count": 3}]
        raise AssertionError(sql)


def _request(**overrides: Any) -> CmcdRequest:
    return CmcdRequest(**{"bucket_seconds": 300, "top_n": 10, **overrides})


def _run(client: _Client, req: CmcdRequest | None = None, sections: set[str] | None = None, **kwargs: Any):
    service = SimpleNamespace(service_id="svc", client=client)
    return cmcd_aggregates(
        service, req or _request(), START, END, ALL_SECTIONS if sections is None else sections, False
    )


@pytest.fixture(autouse=True)
def _asn_names(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(hs_cmcd, "_asn_names", lambda service_id, asns: {7922: "Comcast Cable Communications"})


def test_probe_and_multicall_overview_share_explicit_projected_fact_ownership() -> None:
    client = _Client()
    result = _run(client, sections={"overview"})
    assert len(client.calls) == 4
    assert [row["section"] for row in client.scopes] == [
        "cmcd.probe",
        "cmcd.overview",
        "cmcd.overview",
        "cmcd.overview",
    ]
    assert all(row["source"] == "projected_fact" and row["service_id"] == "svc" for row in client.scopes)
    assert len({row["section_id"] for row in client.scopes[1:]}) == 1
    assert len(result["section_timings"]) == 2
    assert result["section_timings"][0]["section"] == "cmcd.probe"
    hit = _run(client, sections={"overview"})
    assert len(client.calls) == 4
    assert hit["_response_cache"]["state"] == "hit"
    assert hit["section_timings"] == result["section_timings"]


def test_no_cmcd_events_in_the_window_reports_no_data_after_a_single_probe() -> None:
    client = _Client(probe={**_PRESENT, "events": 0, "sessions": 0})

    result = _run(client)
    timings = result.pop("section_timings")
    assert len(timings) == 1
    assert timings[0]["section"] == "cmcd.probe"
    assert timings[0]["source"] == "projected_fact"
    assert timings[0]["outcome"] == "success"

    assert result == {
        "available": True,
        "has_data": False,
        "is_cached": False,
        "_response_cache": {"state": "miss", "ttl_seconds": 30.0, "prewarmer": False, "single_flight": "leader"},
    }
    hit = _run(client)
    assert hit["_response_cache"]["state"] == "hit"
    assert hit["is_cached"] is True
    assert len(client.calls) == 1


def test_every_section_is_computed_from_the_projection() -> None:
    result = _run(_Client())

    assert result["available"] is True and result["has_data"] is True
    assert result["overview"] == {
        "active_sessions": 4,
        "rebuffer_rate": 25.0,
        "avg_bitrate": 2500.13,
        "avg_buffer_length": 12.0,
        "median_throughput": 8000.0,
        "peak_viewers": 4,
        "avg_session_duration": 125,
    }
    assert result["top_content"] == [
        {
            "content_id": "movie-1",
            "session_count": 3,
            "rebuffer_rate": 33.33,
            "avg_bitrate": 3000.0,
            "avg_buffer_length": 9.5,
        }
    ]
    assert result["rebuffer_by_country"] == [
        {"country": "US", "rebuffer_rate": 10.0, "session_count": 3, "median_throughput": 7500.0}
    ]
    assert result["rebuffer_by_asn"] == [
        {
            "asn": 7922,
            "label": "Comcast Cable Communications (7922)",
            "rebuffer_rate": 50.0,
            "session_count": 2,
            "avg_bitrate": 2000.0,
            "avg_buffer_length": 8.0,
        }
    ]
    assert result["object_type_dist"] == [
        {"object_type": "v", "request_count": 30},
        {"object_type": "a", "request_count": 10},
    ]
    assert result["streaming_format_dist"] == [{"streaming_format": "h", "session_count": 4}]
    assert result["session_duration_dist"] == [
        {"duration_bucket": "30s–2m", "session_count": 1},
        {"duration_bucket": "10–30m", "session_count": 3},
    ]


def test_time_series_are_padded_to_every_bucket_in_the_window() -> None:
    result = _run(_Client())

    buckets = [point["bucket"] for point in result["sessions_ts"]]
    assert buckets == [
        "2026-09-14 10:00:00",
        "2026-09-14 10:05:00",
        "2026-09-14 10:10:00",
        "2026-09-14 10:15:00",
    ]
    assert result["sessions_ts"][1] == {
        "bucket": "2026-09-14 10:05:00",
        "concurrent_sessions": 3,
        "rebuffer_session_pct": 33.33,
        "new_sessions": 2,
    }
    assert result["sessions_ts"][0] == {
        "bucket": "2026-09-14 10:00:00",
        "concurrent_sessions": 0,
        "rebuffer_session_pct": None,
        "new_sessions": 0,
    }
    assert result["buffer_health_ts"][1] == {
        "bucket": "2026-09-14 10:05:00",
        "p50_buffer": 10.0,
        "p95_buffer": 20.0,
        "starvation_rate": 5.0,
    }
    assert result["bitrate_ts"][1]["utilization_ratio"] == 0.51
    assert result["throughput_ts"][1] == {"bucket": "2026-09-14 10:05:00", "p50": 7000.0, "p95": 9000.0, "p99": 9500.0}
    assert result["startup_ts"][1] == {"bucket": "2026-09-14 10:05:00", "startup_ratio": 2.5}
    assert result["startup_ts"][3] == {"bucket": "2026-09-14 10:15:00", "startup_ratio": None}


def test_the_four_time_series_share_one_scan() -> None:
    client = _Client()

    _run(client, sections={"sessions_ts", "buffer_health_ts", "bitrate_ts", "throughput_ts", "startup_ts"})

    assert [sql for sql, _ in client.calls if "cmcd:timeseries" in sql] != []
    assert len([sql for sql, _ in client.calls if "cmcd:timeseries" in sql]) == 1


def test_only_requested_sections_are_queried() -> None:
    client = _Client()

    result = _run(client, sections={"object_type_dist"})

    assert [sql.split("*/")[0].split("/*")[1].strip() for sql, _ in client.calls] == [
        "cmcd:probe",
        "cmcd:object_type_dist",
    ]
    assert "overview" not in result and "top_content" not in result


def test_sections_that_need_an_absent_cmcd_key_are_omitted_like_the_standard_path() -> None:
    client = _Client(probe={**_PRESENT, "has_bs": 0, "has_br": 0, "has_cid": 0, "has_country": 0, "has_asn": 0})

    result = _run(client)

    assert "top_content" not in result
    assert "rebuffer_by_country" not in result
    assert "rebuffer_by_asn" not in result
    assert "bitrate_ts" not in result
    assert result["overview"]["rebuffer_rate"] is None
    assert result["overview"]["avg_bitrate"] is None
    assert result["overview"]["avg_buffer_length"] == 12.0
    markers = " ".join(sql for sql, _ in client.calls)
    assert "cmcd:top_content" not in markers and "cmcd:rebuffer_by_country" not in markers


def test_all_queries_are_scoped_to_the_service_published_batches_and_window() -> None:
    client = _Client()

    _run(client)

    for sql, params in client.calls:
        assert "FROM cmcd_projection_facts" in sql
        assert "service_id={service_id:String}" in sql
        assert "publication_state='visible'" in sql
        assert "high_scale_batch_publications FINAL" in sql and "domain='cmcd'" in sql
        assert params["service_id"] == "svc"
        assert "event_timestamp >= {start:DateTime64(3)}" in sql
        assert "event_timestamp <= {end:DateTime64(3)}" in sql
    assert any(params.get("bucket_seconds") == 300 for _, params in client.calls)
    assert any(params.get("top_n") == 10 for _, params in client.calls)


def test_country_and_asn_filters_are_applied_not_dropped() -> None:
    client = _Client()
    req = _request(
        filters={
            "country": {"mode": "include", "values": ["US", "CA"]},
            "asn": {"mode": "exclude", "values": ["7922"]},
        }
    )

    _run(client, req, sections={"overview"})

    for sql, params in client.calls:
        assert "country IN ({flt_0_0:String}, {flt_0_1:String})" in sql
        assert "asn NOT IN ({flt_1_0:UInt32})" in sql
        assert params["flt_0_0"] == "US" and params["flt_0_1"] == "CA"
        assert params["flt_1_0"] == 7922


def test_filters_the_projection_cannot_honour_raise_instead_of_being_ignored() -> None:
    with pytest.raises(ValueError, match="url"):
        _run(_Client(), _request(filters={"url": {"mode": "include", "values": ["/a"]}}))


def test_non_numeric_asn_filter_values_are_rejected() -> None:
    with pytest.raises(ValueError, match="asn"):
        _run(_Client(), _request(filters={"asn": {"mode": "include", "values": ["AS7922"]}}))


def test_output_validates_against_the_standard_response_model() -> None:
    result = _run(_Client())

    model = CmcdAggregatesResponse.model_validate(result)

    assert model.overview is not None and model.overview.peak_viewers == 4
    assert model.rebuffer_by_asn[0].label.endswith("(7922)")


def test_probe_failures_surface_instead_of_becoming_empty_charts() -> None:
    class _Down(_Client):
        def execute(self, sql: str, params: dict | None = None) -> list[dict]:
            raise RuntimeError("clickhouse unavailable")

    with pytest.raises(RuntimeError, match="unavailable"):
        _run(_Down())


def test_bucket_labels_use_the_clickhouse_minute_specifier() -> None:
    client = _Client()

    _run(client, sections={"sessions_ts"})

    timeseries = next(sql for sql, _ in client.calls if "cmcd:timeseries" in sql)
    assert "'%Y-%m-%d %H:%i:%S'" in timeseries
    assert "%M" not in timeseries
