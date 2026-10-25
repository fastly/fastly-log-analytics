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


# ── Merged from test_cmcd_projection_wiring.py ───────────────────────

import gzip
import json
import logging
from datetime import UTC, datetime
from unittest.mock import MagicMock

from backend.high_scale.archive_publication import ArchivePublication, InMemoryObjectStore
from backend.high_scale.ingest_controller import HighScaleIngestController
from backend.high_scale.models import ArchiveState
from backend.high_scale.publication import (
    ClickHousePublication,
    HighScaleBatch,
    InMemoryBatchManifestStore,
    InsertReceipt,
    _rows_for_domain,
)
from backend.high_scale.schema import build_batch_id_from_manifest_id, build_cmcd_projection_key
from backend.high_scale.source_discovery import SourceObjectDescriptor
from tests.high_scale.conftest import HighScaleLedger, OwnershipStore


class _ClickHouse:
    def __init__(self, *, fail_domains: tuple[str, ...] = ()) -> None:
        self.batches: list[HighScaleBatch] = []
        self._fail_domains = fail_domains

    def insert(self, batch: HighScaleBatch) -> InsertReceipt:
        if batch.domain in self._fail_domains:
            raise RuntimeError(f"insert failed for {batch.domain}")
        self.batches.append(batch)
        return InsertReceipt(batch.batch_id, len(batch.rows), batch.digest)

    def by_domain(self, domain: str) -> list[HighScaleBatch]:
        return [b for b in self.batches if b.domain == domain]


def _controller(clickhouse: _ClickHouse, *, control=None) -> HighScaleIngestController:
    ownership = OwnershipStore()
    ownership.initialize("svc", owner="high_scale", source_cursor="cursor-0")
    return HighScaleIngestController(
        ownership=ownership,
        ledger=HighScaleLedger(),
        archive=ArchivePublication(InMemoryObjectStore()),
        serving=ClickHousePublication(InMemoryBatchManifestStore(), clickhouse),
        control_plane=control,
        worker_id="worker-1",
        deletion_grace_seconds=60,
    )


def _payload(*lines: dict) -> bytes:
    return gzip.compress("\n".join(json.dumps(line) for line in lines).encode())


_CMCD_LINE = {"timestamp": "2026-09-11T20:00:00Z", "url": "/seg/1.m4s", "cmcd_br": "1200", "cmcd_sid": "sess-1"}
_PLAIN_LINE = {"timestamp": "2026-09-11T20:00:01Z", "url": "/index.html"}


def _ingest(controller: HighScaleIngestController, *lines: dict, domain: str = "request"):
    payload = _payload(*lines)
    desc = SourceObjectDescriptor("raw/request/one.gz", "sha256:one", len(payload), "v1")
    return controller.ingest_batch(
        service_id="svc",
        domain=domain,
        sources_payloads=[(desc, payload)],
        now=datetime(2026, 9, 11, 20, 0, tzinfo=UTC),
    )


def test_request_ingest_publishes_cmcd_projection_rows_for_cmcd_events_only() -> None:
    clickhouse = _ClickHouse()

    result = _ingest(_controller(clickhouse), _CMCD_LINE, _PLAIN_LINE)

    assert result.publication.rows_visible == 2
    (request_batch,) = clickhouse.by_domain("request")
    (cmcd_batch,) = clickhouse.by_domain("cmcd")
    cmcd_event = next(row for row in request_batch.rows if row["cmcd"])
    (projection,) = cmcd_batch.rows
    assert projection["request_event_id"] == cmcd_event["event_id"]
    assert projection["projection_key"] == build_cmcd_projection_key({"request_event_id": cmcd_event["event_id"]})
    assert projection["fields"]["br"] == "1200"
    assert projection["service_id"] == "svc"
    assert cmcd_batch.generation == request_batch.generation


def test_projection_rows_are_accepted_by_the_cmcd_fact_table_mapping() -> None:
    clickhouse = _ClickHouse()
    _ingest(_controller(clickhouse), _CMCD_LINE)

    (cmcd_batch,) = clickhouse.by_domain("cmcd")

    (row,) = _rows_for_domain(cmcd_batch, "00000000-0000-0000-0000-000000000001")
    assert row[0] == "svc"


def test_requests_without_cmcd_fields_publish_no_projection_batch() -> None:
    clickhouse = _ClickHouse()

    _ingest(_controller(clickhouse), _PLAIN_LINE)

    assert clickhouse.by_domain("cmcd") == []
    assert len(clickhouse.by_domain("request")) == 1


def test_non_request_domains_do_not_project_cmcd() -> None:
    clickhouse = _ClickHouse()
    line = {"timestamp": "2026-09-11T20:00:00Z", "metric_name": "LCP", "metric_value": 1.5, "cmcd_br": "1"}

    _ingest(_controller(clickhouse), line, domain="rum_vitals")

    assert clickhouse.by_domain("cmcd") == []


def test_projection_failure_never_fails_the_request_ingest(caplog: pytest.LogCaptureFixture) -> None:
    clickhouse = _ClickHouse(fail_domains=("cmcd",))

    with caplog.at_level(logging.ERROR, logger="backend.high_scale.ingest_controller"):
        result = _ingest(_controller(clickhouse), _CMCD_LINE)

    assert result.publication.rows_visible == 1
    assert len(clickhouse.by_domain("request")) == 1
    assert any("cmcd projection publish failed" in r.getMessage() for r in caplog.records)


def test_republishing_the_same_batch_id_is_idempotent() -> None:
    clickhouse = _ClickHouse()
    controller = _controller(clickhouse)
    events = [{"event_id": "evt-1", "service_id": "svc", "timestamp": "2026-09-11T20:00:00Z", "cmcd": {"br": "1"}}]

    controller._publish_cmcd_projection("svc", "batch-1", 1, events)
    controller._publish_cmcd_projection("svc", "batch-1", 1, events)

    assert len(clickhouse.by_domain("cmcd")) == 1


def test_malformed_events_are_skipped_without_losing_valid_ones(caplog: pytest.LogCaptureFixture) -> None:
    clickhouse = _ClickHouse()
    controller = _controller(clickhouse)
    good = {"event_id": "evt-1", "service_id": "svc", "timestamp": "2026-09-11T20:00:00Z", "cmcd": {"br": "1"}}
    not_a_mapping = {"event_id": "evt-2", "service_id": "svc", "timestamp": "2026-09-11T20:00:01Z", "cmcd": "br=1"}
    no_event_id = {"service_id": "svc", "timestamp": "2026-09-11T20:00:02Z", "cmcd": {"br": "1"}}

    with caplog.at_level(logging.WARNING, logger="backend.high_scale.ingest_controller"):
        controller._publish_cmcd_projection("svc", "batch-2", 1, [good, not_a_mapping, no_event_id])

    (cmcd_batch,) = clickhouse.by_domain("cmcd")
    assert [row["request_event_id"] for row in cmcd_batch.rows] == ["evt-1"]
    assert any(getattr(r, "skipped", None) == 2 for r in caplog.records)


def test_batch_ingest_publishes_a_manifest_scoped_cmcd_batch() -> None:
    control = MagicMock()
    control.owner.return_value = MagicMock(current_owner="high_scale", owner_epoch=1)
    control.register_archive_manifest.return_value = MagicMock(state=ArchiveState.ARTIFACT_UPLOADING)
    control.archive_manifest.side_effect = KeyError("not found yet")
    clickhouse = _ClickHouse()
    controller = _controller(clickhouse, control=control)
    sources = []
    for index, line in enumerate((_CMCD_LINE, _PLAIN_LINE, {**_CMCD_LINE, "url": "/seg/2.m4s"})):
        payload = _payload(line)
        descriptor = MagicMock(object_key=f"raw/request/part_{index}.gz", checksum=f"sha256:{index}", version="v1")
        descriptor.size_bytes = len(payload)
        sources.append((descriptor, payload))

    result = controller.ingest_batch(service_id="svc", domain="request", sources_payloads=sources, next_cursor="c-3")

    (cmcd_batch,) = clickhouse.by_domain("cmcd")
    assert cmcd_batch.batch_id == build_batch_id_from_manifest_id(f"{result.manifest.manifest_id}:cmcd")
    assert len(cmcd_batch.rows) == 2
    assert len(clickhouse.by_domain("request")[0].rows) == 3
