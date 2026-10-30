from __future__ import annotations

import gzip
import json
import logging
import math
import os
import pathlib
import random
import re
import subprocess
import time
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from email.parser import BytesParser
from email.policy import default
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest
import structlog
from structlog.testing import capture_logs

import backend.high_scale.archive as disk_sampler
from backend.config import ClickHouseConfig
from backend.core.clickhouse import ClickHouseClient
from backend.core.rum_ingest import extract_metrics_from_faro_payload
from backend.cron.jobs import metric_snapshots
from backend.cron.jobs.metric_snapshots import _run_metric_snapshot, _sample_high_scale_disk
from backend.high_scale import cmcd as hs_cmcd
from backend.high_scale import sessions as hs_sessions
from backend.high_scale.archive import (
    ArchiveArtifact,
    ArchiveManifest,
    ArchiveSourceEntry,
    ArchiveSourceObject,
    ArchiveState,
    ClickHousePublication,
    ExportManager,
    ExportState,
    HighScaleBatch,
    HighScaleConfig,
    InMemoryBatchManifestStore,
    PartialInsert,
    PublicationStatus,
    ServiceProfile,
    ServingWatermark,
    StorageProfile,
    VolumeStats,
    _rows_for_domain,
    build_batch_id_from_manifest_id,
    build_cmcd_projection_key,
    build_request_event_id,
    build_rum_event_id,
    build_topology_profile,
    evaluate_disk_state,
    recommend_retention,
    row_matches_serving_domain,
    validate_archive_transition,
    write_batch_archive_checkpoint,
)
from backend.high_scale.cmcd import cmcd_aggregates, project_cmcd
from backend.high_scale.dashboard import aggregates as dashboard_aggregates
from backend.high_scale.ingest_controller import (
    HighScaleService,
    HighScaleServiceRegistry,
    register_high_scale_services,
    register_high_scale_services_from_environment,
)
from backend.high_scale.insights import cache_collapse_detail_endpoint, insights_endpoint
from backend.high_scale.origin import aggregates as origin_aggregates
from backend.high_scale.postgres_control import PostgresControlPlane
from backend.high_scale.query import decode_source_object, query_endpoint
from backend.high_scale.query_service import QueryResponseMetadata, build_origin_projection_rows, query_request_facts
from backend.high_scale.recovery import RecoveryCapacityMeasurement, evaluate_recovery_capacity
from backend.high_scale.sessions import sessions_endpoint
from backend.models.analytics import (
    AggregatesRequest,
    CacheCollapseDetailRequest,
    CmcdRequest,
    InsightsRequest,
    QueryRequest,
    SessionsRequest,
)
from backend.models.common import FilterSpec
from backend.repositories.insights import registry
from backend.repositories.query import MAX_QUERY_ROWS
from backend.utils.remote_access import _is_blocked_path


def identity(ordinal: int = 0, **overrides: Any) -> dict[str, Any]:
    return {
        "service_id": "svc",
        "domain": "request",
        "source_object_key": "raw/request/a.gz",
        "source_object_version": "sha256:a",
        "line_ordinal": ordinal,
        "transform_version": "request.v1",
        **overrides,
    }


_base_row = identity


def test_request_identity_is_distinct_by_ordinal_and_idempotent_on_replay() -> None:
    first, second, replay = (
        build_request_event_id(identity(1)),
        build_request_event_id(identity(2)),
        build_request_event_id(identity(1)),
    )
    assert first != second and first == replay


def test_rum_identities_are_independent_for_vitals_and_errors() -> None:
    row = identity(1)
    vitals_id = build_rum_event_id(row, rum_kind="vitals")
    errors_id = build_rum_event_id(row, rum_kind="errors")
    assert vitals_id != errors_id and uuid.UUID(vitals_id) and uuid.UUID(errors_id)


def test_cmcd_is_derived_from_request_identity() -> None:
    request_id = build_request_event_id(identity(1))
    cmcd_key = build_cmcd_projection_key({"request_event_id": request_id})
    assert cmcd_key == build_cmcd_projection_key({"request_event_id": request_id})
    assert len(uuid.UUID(cmcd_key).bytes) == 16
    with pytest.raises(ValueError, match="request_event_id"):
        build_cmcd_projection_key({})


def test_archive_manifest_and_watermark_validate() -> None:
    source = ArchiveSourceObject("svc", "request", "raw/request/a.gz", "sha256:a", 10, "v1")
    artifact = ArchiveArtifact(
        "s3://bucket/archive/a.parquet", "sha256:b", 20, 2, 100, "sha256:events", "request.v1", "normalize.v1"
    )
    manifest = ArchiveManifest(
        "manifest-1",
        source,
        artifact,
        datetime(2026, 9, 1),
        datetime(2026, 9, 1, 0, 1),
        datetime(2026, 10, 1),
        datetime(2026, 10, 2),
        1,
    )
    manifest.validate()
    ServingWatermark(
        "svc", "request", 1, manifest.coverage_start, manifest.coverage_end, "cursor", "event-1", None, True
    ).validate()


def test_clickhouse_schema_separates_domains_and_fences_visibility() -> None:
    root = Path(__file__).parents[2] / "backend/high_scale/sql"
    request_sql = (root / "request_schema.sql").read_text()
    rum_sql = (root / "rum_schema.sql").read_text()
    cmcd_sql = (root / "cmcd_schema.sql").read_text()
    publication_sql = (root / "publication_schema.sql").read_text()
    assert "ReplicatedMergeTree" in request_sql and "publication_state" in request_sql
    assert "rum_vitals_facts" in rum_sql and "rum_error_facts" in rum_sql
    for table in ("rum_vitals_facts", "rum_error_facts"):
        for col, col_type in (
            ("browser", "LowCardinality(String)"),
            ("os", "LowCardinality(String)"),
            ("device", "LowCardinality(String)"),
            ("city", "String"),
            ("region", "LowCardinality(String)"),
            ("pop", "LowCardinality(String)"),
            ("tls", "LowCardinality(String)"),
            ("ttfb", "Nullable(Float64)"),
        ):
            assert f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col} {col_type}" in rum_sql
    for col in ("error_line", "error_col"):
        assert f"ALTER TABLE rum_error_facts ADD COLUMN IF NOT EXISTS {col} UInt32" in rum_sql
    for expected in (
        "request_event_id",
        "country LowCardinality(String)",
        "ALTER TABLE cmcd_projection_facts ADD COLUMN IF NOT EXISTS country LowCardinality(String)",
        "ALTER TABLE cmcd_projection_facts ADD COLUMN IF NOT EXISTS asn Nullable(UInt32)",
    ):
        assert expected in cmcd_sql
    assert (
        "ReplicatedReplacingMergeTree" in publication_sql
        and "publication_state" in publication_sql
        and "quorum_acked" in publication_sql
    )


def test_origin_projection_schema_is_bounded_and_mergeable() -> None:
    sql = (Path(__file__).parents[2] / "backend/high_scale/sql/origin_schema.sql").read_text()
    for required in (
        "origin_minute_summary",
        "origin_minute_dimensions",
        "publication_state",
        "bucket_start",
        "dimension",
        "value",
        "requests",
        "misses",
        "passes",
        "origin_5xx",
        "status_count",
        "origin_bytes",
        "latency_count",
        "ttlb_count",
        "overhead_count",
        "origin_bytes_count",
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
        "overhead_sk_w",
        "origin_bytes_sk_v",
        "ReplicatedMergeTree",
    ):
        assert required in sql
    for table in ("origin_minute_summary", "origin_minute_dimensions"):
        assert f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS latency_sk_v Array(Float64)" in sql


def test_performance_projection_schema_covers_waterfall_and_scatter() -> None:
    sql = (Path(__file__).parents[2] / "backend/high_scale/sql/performance_schema.sql").read_text()
    for required in (
        "performance_minute_dimensions",
        "performance_minute_summary",
        "performance_scatter_samples",
        "component_count",
        "edge_processing_sum_ms",
        "origin_wait_sum_ms",
        "origin_download_sum_ms",
        "client_download_sum_ms",
        "sample_key UInt64",
        "origin_ms Float64",
        "edge_ms Float64",
        "publication_state",
        "TTL bucket_start",
        "ReplicatedMergeTree",
    ):
        assert required in sql
    assert sql.count("CREATE TABLE IF NOT EXISTS") == 3


# ── Merged from test_event_id.py ──


def test_event_id_is_128_bit_valid_uuid_and_deterministic() -> None:
    event_id = build_request_event_id(_base_row())
    parsed = uuid.UUID(event_id)
    assert (str(parsed) == event_id or event_id == parsed.hex) and len(parsed.bytes) == 16
    assert build_request_event_id(_base_row(line_ordinal=42)) == build_request_event_id(_base_row(line_ordinal=42))


@pytest.mark.parametrize(
    ("a", "b"),
    [
        ({"line_ordinal": 0}, {"line_ordinal": 1}),
        ({"service_id": "ab", "source_object_key": "c"}, {"service_id": "a", "source_object_key": "bc"}),
        ({"source_object_version": "etag_v1"}, {"source_object_version": "etag_v2"}),
        ({"transform_version": "request.v1"}, {"transform_version": "request.v2"}),
    ],
)
def test_event_id_differs_across_identity_components(a: dict[str, Any], b: dict[str, Any]) -> None:
    assert build_request_event_id(_base_row(**a)) != build_request_event_id(_base_row(**b))


# ── Merged from test_batch_idempotency.py ──


def test_batch_id_is_deterministically_derived_from_manifest_id() -> None:
    b1 = build_batch_id_from_manifest_id("manifest-test-abc-123")
    assert b1 == build_batch_id_from_manifest_id("manifest-test-abc-123")
    assert str(uuid.UUID(b1)) == b1
    assert build_batch_id_from_manifest_id("manifest-1") != build_batch_id_from_manifest_id("manifest-2")


def test_publication_mismatch_blocks_visibility() -> None:
    def check_publication(expected: int, visible: int) -> str:
        if expected != visible:
            raise ValueError(f"Publication gate failure: expected {expected} rows, but got {visible}")
        return "visible"

    with pytest.raises(ValueError, match="Publication gate failure"):
        check_publication(100, 95)


# ── Merged from test_field_registry_reuse.py ──


def test_request_field_normalization_parity_with_field_registry() -> None:
    source = ArchiveSourceObject("test_service", "request", "raw/request/test.log.gz", "sha256:123", 100)
    payload = json.dumps(
        {
            "timestamp": "2026-10-09T12:00:00Z",
            "client_ip": "192.0.2.1",
            "status": "200",
            "resp_body_size": "1024",
            "url": "/test",
            "fastly_pop": "SFO",
            "custom_header": "custom_val",
            "cmcd_br": "2000",
        }
    ).encode("utf-8")
    result = decode_source_object(source, payload, transform_version="request.v1")
    assert result.accepted_rows == 1
    ev = result.events[0]
    assert (isinstance(ev["status"], int) or ev["status"] == 200) and (
        isinstance(ev["resp_body_size"], int) or ev["resp_body_size"] == 1024
    )
    assert ev["fastly_pop"] == "SFO" and ev["client_ip"] == "192.0.2.1"


def test_unknown_fields_and_cmcd_routed_to_maps() -> None:
    source = ArchiveSourceObject("test_service", "request", "raw/request/test.log.gz", "sha256:123", 100)
    payload = json.dumps(
        {
            "timestamp": "2026-10-09T12:00:00Z",
            "url": "/stream.mpd",
            "user_tag": "vip",
            "cmcd_sid": "session-1",
            "cmcd_br": "5000",
        }
    ).encode("utf-8")
    result = decode_source_object(source, payload, transform_version="request.v1")
    assert result.accepted_rows == 1
    ev = result.events[0]
    assert ev["custom_fields"].get("user_tag") == "vip"
    assert ev["cmcd"].get("sid") == "session-1" or ev["cmcd"].get("cmcd_sid") == "session-1"


# ── Merged from test_no_hardcoded_database.py ──

_HIGH_SCALE_DIR = pathlib.Path(__file__).resolve().parents[2] / "backend" / "high_scale"
_FORBIDDEN = "fastly_log_analytics."


def test_high_scale_readers_do_not_hardcode_prod_database_name() -> None:
    offenders = [
        f"{py.name}:{lineno}: {line.strip()}"
        for py in sorted(_HIGH_SCALE_DIR.glob("*.py"))
        for lineno, line in enumerate(py.read_text(encoding="utf-8").splitlines(), start=1)
        if _FORBIDDEN in line
    ]
    assert not offenders, f"Hardcoding '{_FORBIDDEN}' breaks non-prod databases:\n" + "\n".join(offenders)


# ── Merged from test_batch_manifest.py ──


def _base_artifact(row_count: int = 100, byte_count: int = 5000) -> ArchiveArtifact:
    return ArchiveArtifact(
        uri="s3://test-bucket/high-scale/archive/manifest-1.parquet",
        checksum="sha256:artifact123",
        size_bytes=4096,
        row_count=row_count,
        byte_count=byte_count,
        canonical_digest="sha256:digest123",
        schema_version="request.v1",
        transform_version="normalize.v1",
    )


def _batch_manifest(
    manifest_id: str, sources: tuple[ArchiveSourceEntry, ...], now: datetime, row_count: int = 100
) -> ArchiveManifest:
    return ArchiveManifest(
        manifest_id=manifest_id,
        service_id="svc_test",
        domain="request",
        sources=sources,
        artifact=_base_artifact(row_count=row_count),
        coverage_start=now - timedelta(minutes=5),
        coverage_end=now,
        retention_deadline=now + timedelta(days=30),
        deletion_authorization_deadline=now + timedelta(days=30),
        archive_epoch=1,
    )


def test_batch_manifest_supports_n_sources_and_validates() -> None:
    now = datetime.now(UTC)
    sources = tuple(
        ArchiveSourceEntry(
            f"raw/request/2026-10-09/part_{i}.gz",
            f"sha256:chk_{i}",
            1024,
            f"v_{i}",
            i * 20,
            20,
            now + timedelta(days=7),
        )
        for i in range(5)
    )
    manifest = _batch_manifest("manifest-batch-001", sources, now)
    manifest.validate()
    assert len(manifest.sources) == 5 and manifest.sources[0].row_start == 0 and manifest.sources[4].row_start == 80


def test_batch_manifest_rejects_row_overflow() -> None:
    now = datetime.now(UTC)
    sources = (
        ArchiveSourceEntry(
            "raw/request/part_0.gz",
            "sha256:chk_0",
            1024,
            row_start=0,
            row_count=60,
            deletion_deadline=now + timedelta(days=7),
        ),
        ArchiveSourceEntry(
            "raw/request/part_1.gz",
            "sha256:chk_1",
            1024,
            row_start=60,
            row_count=50,
            deletion_deadline=now + timedelta(days=7),
        ),
    )
    with pytest.raises(ValueError, match="exceeds artifact row_count"):
        _batch_manifest("manifest-batch-002", sources, now).validate()


def test_independent_per_source_deletion_deadlines() -> None:
    now = datetime.now(UTC)
    s1 = ArchiveSourceEntry(
        "raw/request/file1.gz",
        "sha256:chk1",
        500,
        row_start=0,
        row_count=50,
        deletion_deadline=now - timedelta(minutes=1),
    )
    s2 = ArchiveSourceEntry(
        "raw/request/file2.gz",
        "sha256:chk2",
        600,
        row_start=50,
        row_count=50,
        deletion_deadline=now + timedelta(hours=1),
    )
    manifest = _batch_manifest("manifest-batch-004", (s1, s2), now)
    assert manifest.is_source_eligible_for_deletion("raw/request/file1.gz", now=now) is True
    assert manifest.is_source_eligible_for_deletion("raw/request/file2.gz", now=now) is False


def test_write_batch_archive_checkpoint_end_to_end(tmp_path: Path) -> None:
    now = datetime.now(UTC)
    s1 = ArchiveSourceObject("svc_test", "request", "raw/request/part_0.gz", "sha256:chk0", 1024)
    s2 = ArchiveSourceObject("svc_test", "request", "raw/request/part_1.gz", "sha256:chk1", 2048)
    events_1 = [{"event_id": f"s1_{i}", "status": 200, "client_ip": "1.1.1.1"} for i in range(15)]
    events_2 = [{"event_id": f"s2_{i}", "status": 404, "client_ip": "2.2.2.2"} for i in range(25)]
    manifest = write_batch_archive_checkpoint(
        tmp_path,
        service_id="svc_test",
        domain="request",
        source_batches=[(s1, events_1), (s2, events_2)],
        archive_epoch=1,
        schema_version="request.v1",
        transform_version="normalize.v1",
        coverage_start=now - timedelta(minutes=1),
        coverage_end=now,
    )
    assert len(manifest.sources) == 2 and manifest.artifact.row_count == 40
    assert (manifest.sources[0].key, manifest.sources[0].row_start, manifest.sources[0].row_count) == (
        "raw/request/part_0.gz",
        0,
        15,
    )
    assert (manifest.sources[1].key, manifest.sources[1].row_start, manifest.sources[1].row_count) == (
        "raw/request/part_1.gz",
        15,
        25,
    )


# ── Merged from test_decoder.py ──


def _source(domain: str = "request", key: str = "raw/request/a.gz", version: str = "v7") -> ArchiveSourceObject:
    return ArchiveSourceObject("svc", domain, key, "sha256:source", 100, version)


def test_decode_assigns_stable_identity_replays_and_preserves_duplicate_payloads() -> None:
    payload = gzip.compress(b'{"url":"/same","status":200}\n{"url":"/same","status":200}\n')
    result = decode_source_object(_source(), payload, transform_version="normalize.v1")
    replay = decode_source_object(_source(), payload, transform_version="normalize.v1")
    assert (result.decoded_rows, result.accepted_rows, result.quarantined_rows) == (2, 2, 0)
    assert result.events[0]["event_id"] != result.events[1]["event_id"]
    assert result.events[0]["event_id"] == replay.events[0]["event_id"]
    assert (result.events[0]["line_ordinal"], result.events[1]["line_ordinal"]) == (0, 1)


def test_decode_preserves_request_ip_and_quarantines_malformed_records_losslessly() -> None:
    ip_res = decode_source_object(
        _source(),
        gzip.compress(b'{"ip":"198.51.100.10","url":"/same","status":200}\n'),
        transform_version="normalize.v1",
    )
    assert ip_res.events[0]["ip"] == "198.51.100.10" and "client_ip" not in ip_res.events[0]

    malformed = b'{"url":"/ok"}\nnot-json\n["not","an","object"]\n'
    q_res = decode_source_object(_source(), gzip.compress(malformed), transform_version="normalize.v1")
    assert (q_res.accepted_rows, q_res.quarantined_rows) == (1, 2)
    assert [item.raw_line for item in q_res.dead_letters] == [b"not-json", b'["not","an","object"]']
    assert json.loads(q_res.events[0]["raw_json"])["url"] == "/ok"


@pytest.mark.parametrize(
    ("domain", "raw_bytes", "expected"),
    [
        (
            "rum_vitals",
            b'{"rum_cid":"cid-1","rum_metric_name":"LCP","rum_metric_value":"123.5","rum_metric_rating":"good","rum_pathname":"/home"}\n',
            {
                "client_id": "cid-1",
                "metric_name": "LCP",
                "metric_value": 123.5,
                "metric_rating": "good",
                "pathname": "/home",
            },
        ),
        (
            "rum_vitals",
            b'{"rum_cid":"","rum_metric_name":"","rum_metric_value":"","rum_metric_rating":"","rum_pathname":"","rum_raw_query":"/beacon?rum_metric_name=LCP&rum_metric_value=123.5&rum_metric_rating=good&cid=cid-1&rum_pathname=%2Fhome"}\n',
            {
                "client_id": "cid-1",
                "metric_name": "LCP",
                "metric_value": 123.5,
                "metric_rating": "good",
                "pathname": "/home",
            },
        ),
        (
            "rum_vitals",
            b'{"rum_metric_name":"CLS","rum_metric_value":"0.02","rum_raw_query":"/beacon?rum_metric_name=LCP&rum_metric_value=999"}\n',
            {"metric_name": "CLS", "metric_value": 0.02},
        ),
        (
            "rum_errors",
            b'{"rum_cid":"cid-2","rum_error_message":"boom","rum_error_file":"app.js","rum_pathname":"/checkout"}\n',
            {"client_id": "cid-2", "error_message": "boom", "error_file": "app.js", "pathname": "/checkout"},
        ),
    ],
)
def test_decode_normalizes_rum_fields_and_querystring_fallback(
    domain: str, raw_bytes: bytes, expected: dict[str, Any]
) -> None:
    res = decode_source_object(
        _source(domain, f"raw/rum/{domain}.gz"), gzip.compress(raw_bytes), transform_version="normalize.v1"
    )
    for k, v in expected.items():
        assert res.events[0][k] == v


def test_decode_expands_faro_rum_body_matching_standard_mode_parity() -> None:
    faro_payload = {
        "meta": {
            "browser": {"name": "Firefox", "mobile": True},
            "os": {"name": "Android"},
            "page": {"url": "https://shop.example.test/products/42?tab=reviews"},
        },
        "measurements": [
            {
                "type": "web-vitals",
                "values": {"lcp": 1820.5, "cls": 0.04, "delta": 12.0},
                "context": {"rating": "good"},
            },
            {"type": "web-vitals", "values": {"inp": 195.0}, "meta": {"rating": "needs-improvement"}},
        ],
        "events": [
            {"name": "faro.performance.navigation", "attributes": {"ttfb": 110.25, "fcp": 640.0}},
            {"name": "add_to_cart_click"},
        ],
        "exceptions": [
            {
                "value": "TypeError: Cannot read properties of null",
                "stacktrace": {
                    "frames": [{"filename": "https://shop.example.test/static/cart.js", "lineno": 88, "colno": 14}]
                },
            },
            {
                "message": "ReferenceError: checkoutToken is not defined",
                "stacktrace": {
                    "frames": [{"filename": "https://shop.example.test/static/pay.js", "lineno": 19, "colno": 3}]
                },
            },
        ],
    }
    raw_record = {
        "timestamp": "2026-04-20T12:00:00Z",
        "rum_cid": "cid-faro-99",
        "fastly_req_id": "req-faro-1",
        "country": "US",
        "rum_body": json.dumps(faro_payload),
    }
    payload_bytes = gzip.compress((json.dumps(raw_record) + "\n").encode("utf-8"))

    standard_extracted = extract_metrics_from_faro_payload(faro_payload, raw_record)
    standard_vitals = [
        m for m in standard_extracted if not ((m.get("metric_name") == "exception") or bool(m.get("error_message")))
    ]
    standard_errors = [
        m for m in standard_extracted if (m.get("metric_name") == "exception") or bool(m.get("error_message"))
    ]
    assert len(standard_vitals) == 6 and len(standard_errors) == 2

    vitals_result = decode_source_object(
        _source("rum_vitals", "raw/rum/faro.gz"), payload_bytes, transform_version="normalize.v1"
    )
    assert (vitals_result.decoded_rows, vitals_result.accepted_rows, vitals_result.quarantined_rows) == (1, 6, 0)
    assert len({ev["event_id"] for ev in vitals_result.events}) == 6
    for ev, std in zip(vitals_result.events, standard_vitals, strict=True):
        assert ev["metric_name"] == std["metric_name"] and ev["metric_value"] == float(std["metric_value"])
        assert ev["metric_rating"] == std["metric_rating"] and ev["pathname"] == std["pathname"] == "/products/42"
        assert ev["client_id"] == std["cid"] == "cid-faro-99" and ev["browser"] == std["browser"] == "Firefox"
        assert (
            ev["os"] == std["os"] == "Android" and ev["device"] == std["device"] == "Mobile" and ev["line_ordinal"] == 0
        )

    errors_result = decode_source_object(
        _source("rum_errors", "raw/rum/faro.gz"), payload_bytes, transform_version="normalize.v1"
    )
    assert (errors_result.decoded_rows, errors_result.accepted_rows, errors_result.quarantined_rows) == (1, 2, 0)
    assert len({ev["event_id"] for ev in errors_result.events}) == 2
    for ev, std in zip(errors_result.events, standard_errors, strict=True):
        for k in ("error_message", "error_file", "error_line", "error_col"):
            assert ev[k] == std[k]
        assert ev["pathname"] == std["pathname"] == "/products/42" and ev["client_id"] == std["cid"] == "cid-faro-99"
        assert ev["browser"] == std["browser"] == "Firefox" and ev["os"] == std["os"] == "Android"
        assert ev["device"] == std["device"] == "Mobile" and ev["line_ordinal"] == 0

    for domain, res, expected_len, batch_uuid in (
        ("rum_vitals", vitals_result, 6, "00000000-0000-0000-0000-000000000001"),
        ("rum_errors", errors_result, 2, "00000000-0000-0000-0000-000000000002"),
    ):
        serving_rows = tuple(e for e in res.events if row_matches_serving_domain(domain, e))
        assert len(serving_rows) == expected_len
        batch = HighScaleBatch(
            batch_id=f"b-{domain}", service_id="svc", domain=domain, generation="gen-1", rows=serving_rows
        )
        ch_tuples = _rows_for_domain(batch, batch_uuid)
        assert len(ch_tuples) == expected_len and len({row[1] for row in ch_tuples}) == expected_len


@pytest.mark.parametrize("claimed_domain", ["rum_vitals", "rum_errors"])
def test_faro_exception_without_message_survives_without_becoming_a_vital(claimed_domain: str) -> None:
    record = {
        "timestamp": "2026-10-25T12:00:00Z",
        "rum_metric_name": "LCP",
        "rum_metric_value": "123",
        "url": "/beacon?rum_metric_name=LCP&rum_metric_value=123",
        "rum_body": json.dumps({"exceptions": [{"stacktrace": {"frames": [{"filename": "app.js"}]}}]}),
    }
    result = decode_source_object(
        _source(claimed_domain, "raw/rum/mixed.gz", "v1"), json.dumps(record).encode(), transform_version="normalize.v1"
    )
    assert result.quarantined_rows == 0 and len(result.events) == 1
    assert row_matches_serving_domain("rum_errors", result.events[0]) and not row_matches_serving_domain(
        "rum_vitals", result.events[0]
    )
    assert result.events[0]["error_message"] == "Unknown error"


def test_flat_error_with_stack_but_empty_message_survives() -> None:
    payload = b'{"timestamp":"2026-10-25T12:00:00Z","rum_error_message":"","rum_error_stack":"at app.js:1"}'
    result = decode_source_object(
        _source("rum_errors", "raw/rum/flat.gz", "v1"), payload, transform_version="normalize.v1"
    )
    assert result.accepted_rows == 1 and row_matches_serving_domain("rum_errors", result.events[0])
    assert result.events[0]["error_message"] == "Unknown error"


@pytest.mark.parametrize(
    ("domain", "payload"),
    [
        (
            "rum_errors",
            json.dumps(
                {
                    "timestamp": "2026-10-25T12:00:00Z",
                    "rum_body": json.dumps({"measurements": [{"type": "web-vitals", "values": {"lcp": "broken"}}]}),
                }
            ).encode(),
        ),
        ("rum_vitals", b'{"timestamp":"2026-10-25T12:00:00Z","rum_error_message":"","rum_metric_value":""}'),
        ("rum_errors", b'{"timestamp":"2026-10-25T12:00:00Z","rum_error_message":"","rum_metric_value":""}'),
        ("rum_errors", json.dumps({"rum_body": json.dumps({"exceptions": [{"value": 42}]})}).encode()),
    ],
)
def test_invalid_rum_records_are_quarantined(domain: str, payload: bytes) -> None:
    result = decode_source_object(
        _source(domain, "raw/rum/invalid.gz", "v1"), payload, transform_version="normalize.v1"
    )
    assert result.accepted_rows == 0 and result.quarantined_rows == 1 and result.dead_letters[0].raw_line == payload


# ── Merged from tests/high_scale/test_clickhouse_readers_integration.py ──

_CH_SKIP = pytest.mark.skipif(
    os.getenv("RUN_HS_READERS_CLICKHOUSE_INTEGRATION") != "1", reason="explicit real ClickHouse readers gate"
)
SQL_ROOT = Path("backend/high_scale/sql")
SERVICE = "svc-it"
NOW = datetime.now(UTC).replace(microsecond=0)


def _schema() -> str:
    parts = []
    for name in ("publication_schema.sql", "request_schema.sql", "cmcd_schema.sql", "origin_schema.sql"):
        text = (SQL_ROOT / name).read_text()
        text = re.sub(
            r"ReplicatedReplacingMergeTree\(([^)]+)\)\s*\('[^']+',\s*'[^']+'\)", r"ReplacingMergeTree(\1)", text
        )
        text = re.sub(r"ReplicatedMergeTree\([^;]*?\)", "MergeTree()", text)
        parts.append(re.sub(r"\nTTL [^;]*;", ";", text))
    return "\n".join(parts)


class _Fixture:
    def __init__(self) -> None:
        self.seed = _schema()

    def insert(self, table: str, domain: str, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        self.seed += f"\nINSERT INTO {table} FORMAT JSONEachRow\n" + "\n".join(json.dumps(r) for r in rows) + "\n;\n"
        publications = [
            {
                "batch_id": batch,
                "service_id": SERVICE,
                "domain": domain,
                "generation": "1",
                "batch_digest": "fixture",
                "expected_rows": 1,
                "visible_rows": 1,
                "quorum_acked": 1,
                "publication_state": "visible",
                "manifest_version": 2,
                "updated_at": NOW.isoformat(),
            }
            for batch in {r["batch_id"] for r in rows}
        ]
        self.seed += (
            "\nINSERT INTO high_scale_batch_publications FORMAT JSONEachRow\n"
            + "\n".join(json.dumps(p) for p in publications)
            + "\n;\n"
        )

    def request(
        self, when: datetime, url: str = "/ok", *, ip: str = "203.0.113.1", country: str = "US", **fields: Any
    ) -> dict[str, Any]:
        cmcd = {k[5:]: str(fields.pop(k)) for k in [k for k in fields if k.startswith("cmcd_")]}
        return {
            "service_id": SERVICE,
            "event_id": str(uuid.uuid4()),
            "event_timestamp": when.isoformat(),
            "ingest_timestamp": when.isoformat(),
            "source_object_key": "raw/fixture.gz",
            "source_object_version": "v1",
            "line_ordinal": 0,
            "transform_version": "t1",
            "batch_id": "11111111-1111-4111-8111-111111111111",
            "publication_state": "visible",
            "country": country,
            "client_ip": ip,
            "url": url,
            "custom_fields": {k: str(v) for k, v in fields.items()},
            "cmcd": cmcd,
        }


def _make_client(seed: str) -> ClickHouseClient:
    def transport(request: httpx.Request) -> httpx.Response:
        form = BytesParser(policy=default).parsebytes(
            f"Content-Type: {request.headers['content-type']}\r\n\r\n".encode() + request.read()
        )
        fields: dict[str, str] = {}
        for part in form.iter_parts():
            name = part.get_param("name", header="content-disposition")
            payload = part.get_payload(decode=True)
            assert isinstance(name, str) and isinstance(payload, bytes)
            fields[name] = payload.decode()
        command = [
            "docker",
            "run",
            "--rm",
            "-i",
            "--entrypoint",
            "clickhouse",
            os.getenv("RUM_CLICKHOUSE_TEST_IMAGE", "clickhouse/clickhouse-server:25.8.4.13"),
            "local",
            "--multiquery",
            "--format=JSON",
            "--date_time_input_format=best_effort",
            "--output_format_json_quote_64bit_integers=0",
        ]
        command += [f"--{name}={value}" for name, value in fields.items() if name.startswith("param_")]
        for name in ("additional_table_filters", "max_result_rows", "result_overflow_mode"):
            if name in request.url.params:
                command.append(f"--{name}={request.url.params[name]}")
        completed = subprocess.run(
            command, input=seed + "\n" + fields["query"] + ";", text=True, capture_output=True, timeout=120
        )
        return httpx.Response(
            500 if completed.returncode else 200,
            content=(completed.stderr if completed.returncode else completed.stdout).encode(),
        )

    return ClickHouseClient(
        ClickHouseConfig(host="localhost", port=8123, database="default", user="default", password=""),
        transport=httpx.MockTransport(transport),
    )


@pytest.fixture
def fixture() -> Iterator[_Fixture]:
    yield _Fixture()


@pytest.fixture
def service(fixture: _Fixture) -> Iterator[SimpleNamespace]:
    client = _make_client(fixture.seed)
    yield SimpleNamespace(service_id=SERVICE, client=client)
    client.close()


@_CH_SKIP
def test_unfiltered_dashboard_counts_exact_subminute_event_window(fixture: _Fixture) -> None:
    start = datetime(2026, 1, 1, 0, 0, 17, 125000, tzinfo=UTC)
    end = start + timedelta(minutes=1)
    rows = [
        fixture.request(start - timedelta(milliseconds=1), "/outside"),
        fixture.request(start, "/kept"),
        fixture.request(start + timedelta(seconds=1), "/kept"),
        fixture.request(end - timedelta(milliseconds=1), "/kept"),
        fixture.request(end, "/outside"),
        fixture.request(end + timedelta(seconds=1), "/outside"),
        {**fixture.request(start, "/other"), "service_id": "other-svc"},
    ]
    fixture.insert("request_facts", "request", rows)
    client = _make_client(fixture.seed)
    try:
        svc = HighScaleService(
            service_id=SERVICE,
            client=client,
            cursor_secret=b"fixture",
            request_watermark=ServingWatermark(SERVICE, "request", 1, start, end, None, None, None, True),
        )
        response = dashboard_aggregates(
            svc,
            AggregatesRequest(fields=["url"], include_time_series=False, include_map_data=False),
            start.isoformat(),
            end.isoformat(),
        )
    finally:
        client.close()

    assert response.total_rows == 3 and response.data["url"].total == 3
    assert [(row.value, row.count) for row in response.data["url"].top] == [("/kept", 3)]


def _spread(start: datetime, end: datetime, count: int) -> list[datetime]:
    step = (end - start) / max(count, 1)
    return [start + step * i for i in range(count)]


@pytest.fixture
def traffic(fixture: _Fixture) -> _Fixture:
    baseline = (NOW - timedelta(hours=11, minutes=40), NOW - timedelta(hours=2, minutes=20))
    window = (NOW - timedelta(hours=1, minutes=40), NOW - timedelta(minutes=1))
    rows: list[dict[str, Any]] = []
    add = rows.append

    def burst(start_end: tuple[datetime, datetime], count: int, url: str = "/ok", **kw: Any) -> None:
        for moment in _spread(*start_end, count):
            add(fixture.request(moment, url, **kw))

    burst(baseline, 400, status=200, cache="HIT", elapsed=100000, ua="Mozilla/5.0", ost=200, ottfb=50000, method="GET")
    burst(window, 100, status=200, cache="HIT", elapsed=100000, ua="Mozilla/5.0", ost=200, ottfb=50000, method="GET")
    burst(baseline, 20, "/err", status=200, cache="HIT", elapsed=100000)
    burst(window, 4, "/err", status=200, cache="HIT", elapsed=100000)
    burst(window, 6, "/err", status=500, cache="MISS", elapsed=100000)
    burst(window, 6, country="NZ", status=200, cache="HIT", elapsed=100000)
    burst(baseline, 36, "/collapse", status=200, cache="HIT")
    burst(baseline, 4, "/collapse", status=200, cache="MISS")
    burst(window, 2, "/collapse", status=200, cache="HIT")
    burst(window, 18, "/collapse", status=200, cache="MISS")
    burst(baseline, 55, "/passflip", status=200, cache="HIT")
    burst(baseline, 5, "/passflip", status=200, cache="PASS")
    burst(window, 2, "/passflip", status=200, cache="HIT")
    burst(window, 18, "/passflip", status=200, cache="PASS")
    burst(baseline, 30, "/slow", status=200, cache="HIT", elapsed=100000)
    burst(window, 10, "/slow", status=200, cache="HIT", elapsed=900000)
    burst(window, 36, "/tail", status=200, cache="HIT", elapsed=10000)
    burst(window, 4, "/tail", status=200, cache="HIT", elapsed=500000)
    burst(window, 300, "/bot", ua="evil-bot/1.0", status=200, cache="HIT", elapsed=100000, asn=64500, method="POST")
    burst(window, 30, "/new-asn", asn=64501, status=200, cache="HIT", elapsed=100000)
    for index in range(30):
        add(fixture.request(window[0] + timedelta(minutes=index), f"/nf/{index}", ip="198.51.100.7", status=404))
    for index in range(25):
        add(
            fixture.request(
                window[0] + timedelta(minutes=index),
                "/login",
                ip="198.51.100.8",
                cookie_session=f"s{index}",
                status=200,
            )
        )
    for index in range(8):
        add(fixture.request(window[0] + timedelta(minutes=index), "/fp", ip=f"192.0.2.{index + 1}", ja4="t13d-shared"))
    burst(window, 10, "/waf", waf_sig="XSS, SQLI", status=403)
    burst(window, 40, "/city", city="Springfield", region="IL", status=200, cache="HIT", elapsed=100000)
    for index in range(30):
        add(fixture.request(window[0] + timedelta(minutes=index), "/login", ip="198.51.100.9", status=401))
    for index, path in enumerate(("/admin", "/.env", "/wp-login", "/admin", "/.env", "/wp-login")):
        add(fixture.request(baseline[0] + timedelta(minutes=30 * index), path, ip="198.51.100.10", status=404))
    for index in range(60):
        add(
            fixture.request(
                window[0] + timedelta(minutes=index % 30, seconds=index),
                f"/a/b/c{index % 12}",
                ip="198.51.100.11",
                status=200,
            )
        )
    for index in range(40):
        add(
            fixture.request(
                window[0] + timedelta(seconds=60 * index),
                "/poll",
                ip="198.51.100.12",
                ua="python-requests/2.31",
                status=200,
            )
        )
    for index in range(3):
        add(
            fixture.request(
                window[0] + timedelta(minutes=index),
                "/spoof",
                ip="198.51.100.13",
                ja4="t13d-spoofed",
                pop="SJC",
                lat=-33.9,
                lon=151.2,
                tcp_rtt=1000,
                edge="1",
                status=200,
            )
        )
    full = dict(
        status=200, ost=200, cache="HIT", elapsed=120000, ottfb=60000, oconnect_ms=30, oretries=1, oip="192.0.2.99",
        ua="Mozilla/5.0", referer="https://example.com/", method="GET", p_type="residential", c_type="broadband",
        c_speed="fast", transport="quic", pop="SJC", server_region="us-west", ja3="j3", ja4="j4", tls_ciphers_sha="abc",
        cookie_session="cs", asn=64510, metro=807, city="San Jose", region="CA", delivery_rate=1000000, resp_bytes=2048,
        req_header_bytes=400, conn_requests=3, tcp_rtt=20000, lat=37.3, lon=-121.9, ploss=0.0, rtt_var=1000, retrans=0,
        ttl=3600, hits=1, age=10, edge="1", rid="r1", prid="", resp_header_content_encoding="gzip", waf_sig="XSS",
    )  # fmt: skip
    for index in range(5):
        add(fixture.request(NOW - timedelta(seconds=30 + index), "/all-fields", **full))
    fixture.insert("request_facts", "request", rows)
    return fixture


def _insight_cards(service: SimpleNamespace) -> dict[str, dict[str, Any]]:
    response = insights_endpoint(service, InsightsRequest(window_size_hrs=2, baseline_hours=10), None, None, False)  # type: ignore[arg-type]
    return {card["id"]: card for card in json.loads(response.model_dump_json(by_alias=True))["insights"]}


@_CH_SKIP
@pytest.mark.timeout(1800)
def test_every_ported_detection_executes_and_planted_anomalies_are_found(
    traffic: _Fixture, service: SimpleNamespace, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "backend.utils.pop_utils.get_pop_lat_lon_map", lambda: {"SJC": (37.36, -121.93), "IAD": (38.94, -77.46)}
    )
    cards = _insight_cards(service)
    assert {card_id: card["summary"] for card_id, card in cards.items() if card["severity"] == "error"} == {}
    fired = {card_id for card_id, card in cards.items() if card["items"]}
    assert {
        "error_spikes",
        "new_country_traffic",
        "cache_collapse",
        "cacheability_regression",
        "latency_regression",
        "tail_latency",
        "ua_monoculture",
        "asn_concentration",
        "new_asn_traffic",
        "content_discovery",
        "method_drift",
        "session_harvesting",
        "botnet_grouping",
        "waf_signal_spikes",
        "city_surges",
        "new_city_traffic",
        "credential_enumeration",
        "low_and_slow",
        "orphaned_deep_crawl",
        "repeated_patterns",
        "impossible_distance",
    } <= fired
    assert set(cards) == {definition.id for definition in registry.get_all()}
    spike = cards["error_spikes"]["items"][0]
    assert spike["label"] == "/err" and spike["meta"]["errors"] == 6
    assert cards["new_country_traffic"]["items"][0]["meta"]["filters"] == {"country": "NZ"}
    assert cards["cache_collapse"]["items"][0]["label"] == "/collapse"
    assert cards["session_harvesting"]["items"][0]["current_val"] == 25
    assert {item["label"] for item in cards["waf_signal_spikes"]["items"]} >= {"XSS", "SQLI"}
    assert cards["city_surges"]["items"][0]["label"].startswith("Springfield")
    assert cards["credential_enumeration"]["items"][0]["current_val"] == 30
    poller = next(item for item in cards["repeated_patterns"]["items"] if item["label"] == "198.51.100.12")
    assert poller["meta"]["mode_gap_s"] == 60 and poller["meta"]["n_events"] == 40 and poller["meta"]["score"] >= 90


@_CH_SKIP
def test_cache_collapse_detail_reads_real_rows(traffic: _Fixture, service: SimpleNamespace) -> None:
    req = CacheCollapseDetailRequest(url="/collapse", window_size_hrs=2, baseline_hours=10)
    result = cache_collapse_detail_endpoint(service, req, None, None, False)  # type: ignore[arg-type]
    assert result.has_data is True and (result.breakdown.hits, result.breakdown.misses) == (2, 18)
    assert result.window_hit_rate == pytest.approx(10.0) and result.baseline_hit_rate == pytest.approx(90.0)
    assert result.timeline and result.recent_misses and all(point.bucket.endswith("Z") for point in result.timeline)


@_CH_SKIP
def test_sessions_group_stitch_count_and_flag_availability(
    traffic: _Fixture, service: SimpleNamespace, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(hs_sessions, "_asn_label_map", lambda service_id, asns: {a: f"AS{a}" for a in asns})
    start, end = (NOW - timedelta(hours=3)).isoformat(), NOW.isoformat()
    result = sessions_endpoint(
        service,  # type: ignore[arg-type]
        SessionsRequest(limit=2, min_reqs_flag=20, sort_by="req_count", sort_dir="desc"),
        start,
        end,
    )
    assert result["total"] >= 3 and len(result["sessions"]) == 2
    counts = [session["req_count"] for session in result["sessions"]]
    assert counts == sorted(counts, reverse=True)
    assert result["has_rtt"] is True and result["has_ja4"] is True and result["has_cmcd"] is False
    past_the_end = sessions_endpoint(service, SessionsRequest(limit=2, page=99), start, end)  # type: ignore[arg-type]
    assert past_the_end["sessions"] == [] and past_the_end["total"] == result["total"]


@_CH_SKIP
def test_cmcd_buckets_line_up_with_the_padded_axis(
    fixture: _Fixture, service: SimpleNamespace, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(hs_cmcd, "_asn_names", lambda service_id, asns: {})
    events = []
    for index in range(40):
        moment = NOW - timedelta(minutes=30) + timedelta(seconds=index * 30)
        event = {
            "event_id": str(uuid.uuid4()),
            "service_id": SERVICE,
            "timestamp": moment.isoformat(),
            "country": "US" if index % 2 else "DE",
            "asn": 7922,
            "cmcd": {
                "sid": f"s{index % 4}",
                "br": "2000",
                "bl": "9000",
                "mtp": "9000",
                "bs": "true" if index % 5 == 0 else "false",
                "ot": "v",
                "cid": "movie",
            },
        }
        projection = project_cmcd(event)
        assert projection is not None
        events.append(
            {
                "service_id": SERVICE,
                "projection_key": projection["projection_key"],
                "request_event_id": event["event_id"],
                "event_timestamp": moment.isoformat(),
                "batch_id": "22222222-2222-4222-8222-222222222222",
                "publication_state": "visible",
                "cmcd": projection["fields"],
                "country": projection["country"],
                "asn": projection["asn"],
            }
        )
    fixture.insert("cmcd_projection_facts", "cmcd", events)
    seeded_client = _make_client(fixture.seed)
    try:
        result = cmcd_aggregates(
            SimpleNamespace(service_id=SERVICE, client=seeded_client),  # type: ignore[arg-type]
            CmcdRequest(bucket_seconds=300, top_n=5),
            (NOW - timedelta(minutes=40)).isoformat(),
            NOW.isoformat(),
            None,
            False,
        )
    finally:
        seeded_client.close()

    assert result["has_data"] is True and result["overview"]["active_sessions"] == 4
    populated = [point for point in result["sessions_ts"] if point["concurrent_sessions"]]
    assert populated and sum(point["new_sessions"] for point in populated) == 4
    assert {row["country"] for row in result["rebuffer_by_country"]} == {"US", "DE"}
    assert result["rebuffer_by_asn"][0]["asn"] == 7922 and result["top_content"][0]["content_id"] == "movie"


@_CH_SKIP
def test_origin_sketches_merge_to_near_exact_percentiles(fixture: _Fixture, monkeypatch: pytest.MonkeyPatch) -> None:
    rng = random.Random(9)
    events = []
    values: list[float] = []
    base = (NOW - timedelta(hours=1)).replace(second=0)
    for minute in range(40):
        for _ in range(60):
            latency = rng.lognormvariate(8, 1.0) * (1 + minute / 20)
            values.append(latency)
            events.append(
                {
                    "timestamp": (base + timedelta(minutes=minute, seconds=rng.randint(0, 59))).isoformat(),
                    "ottfb": latency,
                    "ost": 200,
                    "url": rng.choice(["/a", "/b"]),
                    "pop": "SJC",
                }
            )
    projection = build_origin_projection_rows(tuple(events))
    fixture.insert(
        "origin_minute_summary",
        "origin_summary",
        [
            {
                "service_id": SERVICE,
                "batch_id": "33333333-3333-4333-8333-333333333333",
                "publication_state": "visible",
                **row,
            }
            for row in projection.summary_rows
        ],
    )
    fixture.insert(
        "origin_minute_dimensions",
        "origin_dimensions",
        [
            {
                "service_id": SERVICE,
                "batch_id": "44444444-4444-4444-8444-444444444444",
                "publication_state": "visible",
                **row,
            }
            for row in projection.dimension_rows
        ],
    )
    client = _make_client(fixture.seed)
    try:
        request = SimpleNamespace(
            filters={},
            bucket_minutes=10,
            split_by_leg=False,
            timeseries_metric="ttfb",
            timeseries_percentile="p95",
            slow_urls_limit=5,
            slow_urls_min_requests=1,
            ip_health_limit=5,
            pop_latency_limit=5,
        )
        result = origin_aggregates(
            SimpleNamespace(service_id=SERVICE, client=client),  # type: ignore[arg-type]
            request,
            (NOW - timedelta(hours=2)).isoformat(),
            NOW.isoformat(),
            sections={"summary", "timeseries", "slow_urls", "pop_latency"},
        )
    finally:
        client.close()

    ordered = sorted(values)
    exact_p95 = ordered[math.ceil(0.95 * len(ordered)) - 1] / 1000.0
    assert result["summary"]["ottfb_p95_ms"] == pytest.approx(exact_p95, rel=0.015)
    assert result["summary"]["_approx"] is True and "histogram sketch" in result["summary"]["_approx_note"]
    assert result["timeseries"]["series"] and all(point["value"] for point in result["timeseries"]["series"])
    assert {row["url"] for row in result["slow_urls"]["rows"]} == {"/a", "/b"}
    assert result["pop_latency"]["rows"][0]["pop"] == "SJC"


_ORIGIN_SECTIONS = {"summary", "timeseries", "slow_urls", "status_codes", "path_breakdown", "pop_latency", "ip_health"}


def _filtered_origin(service: SimpleNamespace, filters: dict[str, Any]) -> dict[str, Any]:
    req = SimpleNamespace(
        filters=filters,
        bucket_minutes=60,
        split_by_leg=False,
        timeseries_metric="ttfb",
        timeseries_percentile="p95",
        slow_urls_limit=5,
        slow_urls_min_requests=1,
        ip_health_limit=5,
        pop_latency_limit=5,
    )
    return origin_aggregates(
        service,
        req,
        (NOW - timedelta(hours=12)).isoformat(),
        (NOW + timedelta(minutes=1)).isoformat(),
        sections=_ORIGIN_SECTIONS,
    )


@_CH_SKIP
def test_filtered_origin_is_computed_exactly_from_request_facts(traffic: _Fixture, service: SimpleNamespace) -> None:
    result = _filtered_origin(
        service,
        {"url": FilterSpec(mode="include", values=["/ok"]), "method": FilterSpec(mode="include", values=["GET"])},
    )
    summary = result["summary"]
    assert summary["has_data"] is True and summary["_approx"] is False
    assert summary["total_misses"] == 0 and summary["total_passes"] == 0
    assert summary["ottfb_p50_ms"] == summary["ottfb_p95_ms"] == 50.0 and summary["origin_error_rate"] == 0.0
    assert summary["ottlb_p50_ms"] is None and summary["obytes_p50"] is None
    assert result["status_codes"]["rows"] == [{"status": 200, "count": 500, "pct": 100.0}]
    assert result["slow_urls"]["rows"] == [
        {"url": "/ok", "requests": 500, "p50_ms": 50.0, "p95_ms": 50.0, "p99_ms": 50.0}
    ]
    assert result["timeseries"]["has_data"] and all(p["value"] == 50.0 for p in result["timeseries"]["series"])
    assert sum(p["miss_count"] for p in result["timeseries"]["series"]) == 0
    assert result["path_breakdown"]["has_data"] is False


@_CH_SKIP
def test_filtered_origin_with_no_matching_rows_reports_no_data_without_error(
    traffic: _Fixture, service: SimpleNamespace
) -> None:
    result = _filtered_origin(
        service,
        {"url": FilterSpec(mode="include", values=["/ok"]), "method": FilterSpec(mode="include", values=["POST"])},
    )
    assert result["has_data"] is False and result["summary"]["has_data"] is False
    assert (
        result["slow_urls"]["rows"] == []
        and result["status_codes"]["rows"] == []
        and result["timeseries"]["series"] == []
    )


@_CH_SKIP
def test_filtered_origin_exclude_mode_and_oip_health(traffic: _Fixture, service: SimpleNamespace) -> None:
    result = _filtered_origin(service, {"country": FilterSpec(mode="exclude", values=["NZ"])})
    assert result["summary"]["has_data"] is True
    assert {row["url"] for row in result["slow_urls"]["rows"]} >= {"/ok"}
    assert 200 in {row["status"] for row in result["status_codes"]["rows"]}


@pytest.fixture
def two_tenants(traffic: _Fixture) -> _Fixture:
    rows = [
        {
            **traffic.request(NOW - timedelta(minutes=90, seconds=i), "/other-tenant", status=200),
            "service_id": "other-svc",
            "batch_id": "22222222-2222-4222-8222-222222222222",
        }
        for i in range(50)
    ]
    traffic.insert("request_facts", "request", rows)
    return traffic


def _console(service: SimpleNamespace, sql: str, *, window_minutes: int = 30) -> list[dict[str, Any]]:
    return query_endpoint(
        service,  # type: ignore[arg-type]
        QueryRequest(sql=sql, max_rows=100),
        (NOW - timedelta(minutes=window_minutes)).isoformat(),
        NOW.isoformat(),
        False,
    )["data"]


@_CH_SKIP
@pytest.mark.parametrize(
    "where",
    ["1=1 OR url = '/nope'", "country = 'US' OR 1 = 1", "url = '/ok' OR url != '/ok'"],
    ids=["tautology", "or-country", "complement"],
)
def test_sql_console_cannot_widen_past_the_service_and_window_with_an_or_condition(
    two_tenants: _Fixture, service: SimpleNamespace, where: str
) -> None:
    scoped = _console(service, "SELECT count(*) AS n FROM logs")[0]["n"]
    attacked = _console(service, f"SELECT count(*) AS n FROM logs WHERE {where}")[0]["n"]
    assert 0 < scoped < 300 and attacked == scoped
    assert _console(service, "SELECT count(*) AS n FROM logs WHERE url = '/other-tenant'")[0]["n"] == 0


@_CH_SKIP
@pytest.mark.parametrize(
    "tail",
    ["-- swallow everything after this", "/* c */", "/* /* */ UNION SELECT 1 FROM system.tables\n-- */ */"],
)
def test_sql_console_rejects_comments_so_a_trailing_one_cannot_swallow_scope_or_row_cap(
    two_tenants: _Fixture, service: SimpleNamespace, tail: str
) -> None:
    with pytest.raises(PermissionError):
        _console(service, f"SELECT url FROM logs {tail}")


@_CH_SKIP
def test_sql_console_row_cap_holds_server_side_for_a_huge_explicit_limit(
    two_tenants: _Fixture, service: SimpleNamespace
) -> None:
    rows = _console(service, "SELECT url FROM logs LIMIT 999999999", window_minutes=60 * 12)
    assert 0 < len(rows) <= 100 and MAX_QUERY_ROWS >= len(rows)


# ── Merged from tests/high_scale/test_contracts_and_gates.py ──


def test_archive_states_are_monotonic() -> None:
    validate_archive_transition(ArchiveState.ARTIFACT_UPLOADING, ArchiveState.ARTIFACT_VERIFIED)
    with pytest.raises(ValueError, match="monotonic"):
        validate_archive_transition(ArchiveState.MANIFEST_COMMITTED, ArchiveState.MANIFEST_PREPARED)


def test_stale_epoch_cannot_commit_manifest_or_claim_ack_batches() -> None:
    control = PostgresControlPlane(pool=MagicMock())
    assert hasattr(control, "acknowledge_sources_batch") and hasattr(control, "claim_sources_batch")

    control._locked_owner = MagicMock(return_value=MagicMock(current_owner="high_scale", owner_epoch=5))  # type: ignore[assignment]
    control.transaction = MagicMock()  # type: ignore[assignment]
    control.transaction.return_value.__enter__.return_value = MagicMock()

    manifest = MagicMock(spec=ArchiveManifest, manifest_id="man-1", service_id="svc-1", domain="request")
    manifest.validate.return_value = None
    with pytest.raises(ValueError, match="owner epoch mismatch|stale owner epoch"):
        control.register_archive_manifest(manifest, owner_epoch=4)


def test_publication_attempt_with_mismatched_row_count_rejected() -> None:
    store = InMemoryBatchManifestStore()
    client = MagicMock()
    batch = HighScaleBatch(
        batch_id="batch-mismatch",
        service_id="svc-test",
        domain="request",
        generation="epoch-1",
        rows=tuple({"event_id": f"evt-{i}"} for i in range(100)),
    )
    client.insert.side_effect = PartialInsert(95)
    publication = ClickHousePublication(store, client)
    with pytest.raises(PartialInsert) as exc_info:
        publication.publish(batch)
    assert exc_info.value.rows_inserted == 95
    manifest = store.get("batch-mismatch")
    assert manifest is not None and manifest.status is PublicationStatus.PENDING and manifest.visible_rows == 0


def test_query_filters_visible_state_and_ignores_pending_rows() -> None:
    client = MagicMock()
    client.execute.return_value = [
        {"event_id": "00000000-0000-0000-0000-000000000001", "timestamp": "2026-10-09T12:00:00Z"}
    ]
    now = datetime(2026, 10, 9, 12, 5, tzinfo=UTC)
    watermark = ServingWatermark(
        "svc-test", "request", 1, now - timedelta(minutes=5), now, "cur-1", "evt-1", "evt-1", True
    )
    results = query_request_facts(
        client,
        service_id="svc-test",
        start=now - timedelta(minutes=5),
        end=now,
        cursor_secret=b"test-secret-123456",
        watermark=watermark,
        limit=10,
    )
    assert len(results.rows) == 1 and client.execute.called
    query_sql, _ = client.execute.call_args[0]
    assert "publication_state = 'visible'" in query_sql or "_high_scale_visible = 1" in query_sql


def test_query_metadata_requires_coverage_watermark_and_approximation_error() -> None:
    watermark = ServingWatermark(
        "svc",
        "request",
        2,
        datetime(2026, 9, 1, tzinfo=UTC),
        datetime(2026, 9, 1, 0, 1, tzinfo=UTC),
        "cursor",
        "event-2",
        "event-2",
        True,
    )
    QueryResponseMetadata("complete", True, 1.0, 3.0, watermark, None, None).validate()

    approx_wm = ServingWatermark("svc", "request", 1, None, None, None, None, None, False)
    with pytest.raises(ValueError, match="approximation"):
        QueryResponseMetadata("complete", False, 0.5, 0.0, approx_wm, None, None).validate()


@pytest.mark.parametrize(
    ("insert_rate", "feasible", "limiting_stage"),
    [
        (750_000_000, True, None),
        (400_000_000, False, "clickhouse_insert"),
    ],
)
def test_recovery_capacity_evaluation(insert_rate: int, feasible: bool, limiting_stage: str | None) -> None:
    report = evaluate_recovery_capacity(
        RecoveryCapacityMeasurement(
            events_per_second=2_000_000,
            recovery_window_seconds=15 * 60,
            archive_read_events_per_second=300_000_000,
            decode_events_per_second=300_000_000,
            aggregate_events_per_second=300_000_000,
            clickhouse_insert_events_per_second=insert_rate,
            replication_factor=3,
            live_ingest_reservation=0.2,
        )
    )
    assert report.feasible is feasible
    if feasible:
        assert report.required_events_per_second == pytest.approx(240_000_000)
        assert report.replication_adjusted_insert_events_per_second == pytest.approx(250_000_000)
    else:
        assert report.limiting_stage == limiting_stage


def test_topology_profiles() -> None:
    local = build_topology_profile("local")
    assert (local.clickhouse_replicas_per_shard, local.keeper_members, local.production_ha) == (1, 1, False)
    prod = build_topology_profile("portable-production")
    assert (prod.clickhouse_replicas_per_shard, prod.keeper_members, prod.production_ha) == (2, 3, True)
    with pytest.raises(ValueError, match="unknown"):
        build_topology_profile("unknown-production")


def test_export_is_bounded_and_completes() -> None:
    manager = ExportManager(max_workers=1, max_rows=2, max_bytes=100)
    try:
        job_id = manager.submit("job-1", [{"id": 1}, {"id": 2}], lambda row: str(row["id"]).encode())
        job = manager.wait(job_id)
        assert job.state is ExportState.COMPLETED and job.rows_written == 2 and job.payload == b"1\n2\n"
    finally:
        manager.shutdown()


def test_export_rejects_row_limit_and_duplicate_job() -> None:
    manager = ExportManager(max_workers=1, max_rows=1, max_bytes=100)
    try:
        manager.submit("job-1", [{"id": 1}], lambda row: b"1")
        with pytest.raises(ValueError, match="already exists"):
            manager.submit("job-1", [{"id": 1}], lambda row: b"1")
        job_id = manager.submit("job-2", [{"id": 1}, {"id": 2}], lambda row: b"x")
        assert manager.wait(job_id).state is ExportState.FAILED
    finally:
        manager.shutdown()


def test_export_can_be_cancelled() -> None:
    manager = ExportManager(max_workers=1, max_rows=1000, max_bytes=100_000)
    try:
        job_id = manager.submit("job-1", range(1000), lambda row: (time.sleep(0.001), b"x")[1])
        time.sleep(0.01)
        assert manager.cancel(job_id) is True and manager.wait(job_id).state is ExportState.CANCELLED
    finally:
        manager.shutdown()


def test_high_scale_config_validation() -> None:
    with pytest.raises(ValueError, match="ClickHouse"):
        HighScaleConfig(True, None, None, None).validate()
    HighScaleConfig(False, None, None, None).validate()


def test_retention_recommendation_preserves_disk_reserve_and_accounts_for_fields() -> None:
    rec = recommend_retention(ServiceProfile(100_000, 900), StorageProfile(10 * 1024**4))
    assert rec.hot_days == 1.0 and rec.warm_days >= rec.hot_days

    narrow = recommend_retention(ServiceProfile(100_000, 100), StorageProfile(10**11))
    wide = recommend_retention(ServiceProfile(200_000, 600), StorageProfile(10**11))
    assert wide.hot_days < narrow.hot_days


# ── Merged from test_registry_bootstrap.py ──


class FakeClickHouse:
    def __init__(self) -> None:
        self.request_coverage_end = datetime(2026, 9, 12, tzinfo=UTC)
        self.calls: list[str] = []

    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        assert params is not None and params["service_id"] == "svc"
        self.calls.append(sql)
        if "max(event_timestamp)" in sql:
            return [{"coverage_start": datetime(2026, 9, 1, tzinfo=UTC), "coverage_end": self.request_coverage_end}]
        if "last_visible_event_id" in sql:
            return [{"last_visible_event_id": "00000000-0000-0000-0000-000000000123"}]
        return []


class EmptyClickHouse:
    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        return [{"coverage_start": None, "coverage_end": None, "last_visible_event_id": None}]


class StringClickHouse:
    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        if "max(event_timestamp)" in sql:
            return [{"coverage_start": "2026-09-12 20:50:12.123", "coverage_end": "2026-09-12 20:50:12.123"}]
        return [{"last_visible_event_id": "a4dc715f-733c-4535-8090-3dd5637e9e0b"}]


class FakeCatalog:
    def manifests_covering(self, service_id: str, domain: str, start: Any, end: Any) -> tuple[()]:
        return ()


def _reg_svc(client: Any = None, **kw: Any) -> HighScaleService:
    reg = HighScaleServiceRegistry()
    register_high_scale_services(
        reg,
        client=client or FakeClickHouse(),
        service_ids=("svc",),
        cursor_secret=kw.pop("cursor_secret", b"local-test-secret"),
        **kw,
    )
    svc = reg.resolve("svc")
    assert svc is not None
    return svc


def test_register_high_scale_services_exposes_and_updates_clickhouse_watermark() -> None:
    client = FakeClickHouse()
    svc = _reg_svc(client, owner_epoch=7)
    assert svc.cursor_secret == b"local-test-secret"
    wm = svc.watermark()
    assert (wm.service_id, wm.domain, wm.owner_epoch) == ("svc", "request", 7)
    assert wm.coverage_start == datetime(2026, 9, 1, tzinfo=UTC)
    assert wm.coverage_end == datetime(2026, 9, 12, tzinfo=UTC)
    assert wm.last_visible_event_id == "00000000-0000-0000-0000-000000000123"
    assert len(client.calls) == 2 and all("argMax" not in sql for sql in client.calls)

    client.request_coverage_end = datetime(2026, 9, 22, 4, 20, tzinfo=UTC)
    assert svc.watermark().coverage_end == datetime(2026, 9, 22, 4, 20, tzinfo=UTC)


def test_register_high_scale_services_parses_clickhouse_timestamp_strings_and_empty_tables() -> None:
    wm = _reg_svc(StringClickHouse()).watermark()
    expected_dt = datetime(2026, 9, 12, 20, 50, 12, 123000, tzinfo=UTC)
    assert (wm.coverage_start, wm.coverage_end, wm.last_visible_event_id) == (
        expected_dt,
        expected_dt,
        "a4dc715f-733c-4535-8090-3dd5637e9e0b",
    )

    empty_wm = _reg_svc(EmptyClickHouse()).watermark()
    assert (empty_wm.coverage_start, empty_wm.coverage_end, empty_wm.last_visible_event_id) == (None, None, None)


def test_register_high_scale_services_rejects_empty_secret_and_missing_env_service_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(ValueError, match="high-scale cursor secret is required"):
        _reg_svc(cursor_secret=b"")

    monkeypatch.setenv("HIGH_SCALE_ENABLED", "1")
    monkeypatch.delenv("HIGH_SCALE_SERVICE_IDS", raising=False)
    with pytest.raises(ValueError, match="HIGH_SCALE_SERVICE_IDS"):
        register_high_scale_services_from_environment(HighScaleServiceRegistry(), client=FakeClickHouse())


def test_register_high_scale_services_cold_tier_wiring() -> None:
    catalog, archive = FakeCatalog(), object()
    svc_both = _reg_svc(manifest_catalog=catalog, archive_for=lambda _: archive)
    assert svc_both.manifest_catalog is catalog and svc_both.archive is archive

    svc_no_arch = _reg_svc(manifest_catalog=FakeCatalog(), archive_for=lambda _: None)
    assert svc_no_arch.manifest_catalog is None and svc_no_arch.archive is None

    svc_default = _reg_svc()
    assert svc_default.manifest_catalog is None and svc_default.archive is None


def test_environment_registration_wires_and_degrades_cold_tier(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HIGH_SCALE_ENABLED", "1")
    monkeypatch.setenv("HIGH_SCALE_SERVICE_IDS", "svc")
    monkeypatch.setenv("HIGH_SCALE_CURSOR_SECRET", "local-test-secret")

    reg1, catalog, archive = HighScaleServiceRegistry(), FakeCatalog(), object()
    register_high_scale_services_from_environment(
        reg1, client=FakeClickHouse(), control_factory=lambda: catalog, archive_factory=lambda _: archive
    )
    svc1 = reg1.resolve("svc")
    assert svc1 is not None and svc1.manifest_catalog is catalog and svc1.archive is archive

    reg2 = HighScaleServiceRegistry()
    register_high_scale_services_from_environment(
        reg2,
        client=FakeClickHouse(),
        control_factory=lambda: FakeCatalog(),
        archive_factory=lambda _: (_ for _ in ()).throw(RuntimeError("no FOS source configured")),
    )
    svc2 = reg2.resolve("svc")
    assert svc2 is not None and svc2.manifest_catalog is None and svc2.archive is None


# ── Merged from test_disk_sampler_wiring.py ──

_TB = 1 << 40


class _FakeClickHouse:
    def __init__(self, free_fraction: float, total: int = _TB) -> None:
        self.total = total
        self.free = int(total * free_fraction)
        self.calls = 0

    def storage_health(self) -> dict:
        self.calls += 1
        return {"up": 1, "disk_free": self.free, "disk_total": self.total}


@pytest.fixture(autouse=True)
def _reset_sampler() -> Iterator[None]:
    disk_sampler.reset_for_tests()
    yield
    disk_sampler.reset_for_tests()


def _history(metric: str) -> list[dict[str, Any]]:
    return metric_snapshots.get_batch(since=datetime.now(UTC) - timedelta(hours=1)).get(metric, [])


def test_sampler_reads_real_volume_stats_and_one_batch_floor(monkeypatch: pytest.MonkeyPatch) -> None:
    probe = _FakeClickHouse(free_fraction=0.12)
    snapshot = disk_sampler.sample_disk_state(probe)
    assert probe.calls == 1 and snapshot.state.mode == "protective" and snapshot.total_bytes == _TB
    assert disk_sampler.latest_disk_snapshot() is snapshot

    monkeypatch.setenv("HIGH_SCALE_DISK_ONE_BATCH_BYTES", str(_TB))
    snapshot2 = disk_sampler.sample_disk_state(_FakeClickHouse(free_fraction=0.5))
    assert snapshot2.state.reserve_bytes == _TB and snapshot2.state.mode == "warning"


def test_mode_transitions_are_logged_once_each(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[tuple[str, dict[str, Any]]] = []

    class _Recorder:
        def info(self, event: str, **kw: Any) -> None:
            events.append(("info", {"event": event, **kw}))

        def warning(self, event: str, **kw: Any) -> None:
            events.append(("warning", {"event": event, **kw}))

    monkeypatch.setattr(disk_sampler, "logger", _Recorder())
    monkeypatch.setattr(disk_sampler, "_latest", None)
    disk_sampler.sample_disk_state(_FakeClickHouse(0.50))
    disk_sampler.sample_disk_state(_FakeClickHouse(0.50))
    disk_sampler.sample_disk_state(_FakeClickHouse(0.08))
    changes = [(level, e) for level, e in events if e["event"] == "high_scale.disk_mode_changed"]
    assert [(e["previous_mode"], e["mode"]) for _, e in changes] == [(None, "normal"), ("normal", "emergency")]
    assert changes[-1][0] == "warning"


def test_metric_snapshot_tick_high_scale_vs_standard_and_probe_failure() -> None:
    probe = _FakeClickHouse(free_fraction=0.12)
    with (
        patch("backend.config.is_high_scale_mode", return_value=True),
        patch("backend.core.clickhouse.get_clickhouse_client", return_value=probe),
    ):
        _run_metric_snapshot()
    assert probe.calls == 1
    assert _history("high_scale_disk_mode_level")[-1]["value"] == 2.0
    assert _history("high_scale_disk_free_fraction")[-1]["value"] == pytest.approx(0.12, abs=1e-3)

    disk_sampler.reset_for_tests()
    with (
        patch("backend.config.is_high_scale_mode", return_value=False),
        patch("backend.core.clickhouse.get_clickhouse_client") as get_client,
    ):
        assert _sample_high_scale_disk() == 0
        _run_metric_snapshot()
    get_client.assert_not_called()
    assert disk_sampler.latest_disk_snapshot() is None

    class _Down:
        def storage_health(self) -> dict:
            raise RuntimeError("clickhouse unavailable")

    with (
        capture_logs() as logs,
        patch("backend.config.is_high_scale_mode", return_value=True),
        patch("backend.core.clickhouse.get_clickhouse_client", return_value=_Down()),
    ):
        assert _sample_high_scale_disk() == 0
    assert any(e["event"] == "high_scale.disk_sample_failed" and e["error_kind"] == "RuntimeError" for e in logs)
    assert disk_sampler.latest_disk_snapshot() is None


def test_health_snapshot_disk_states(client: Any) -> None:
    with patch("backend.config.is_high_scale_mode", return_value=False):
        assert "high_scale" not in client.get("/api/admin/health-snapshot").json()

    with patch("backend.config.is_high_scale_mode", return_value=True):
        assert client.get("/api/admin/health-snapshot").json()["high_scale"]["disk"] is None

    probe = _FakeClickHouse(free_fraction=0.09)
    with (
        patch("backend.config.is_high_scale_mode", return_value=True),
        patch("backend.core.clickhouse.get_clickhouse_client", return_value=probe),
    ):
        _run_metric_snapshot()
        resp = client.get("/api/admin/health-snapshot")
    assert resp.status_code == 200
    disk = resp.json()["high_scale"]["disk"]
    assert disk["mode"] == "emergency" and disk["free_fraction"] == pytest.approx(0.09, abs=1e-3)
    assert disk["total_bytes"] == _TB and disk["sample_age_s"] >= 0


@pytest.mark.parametrize(
    ("free_fraction", "one_batch_bytes", "expected_mode", "expected_reserve"),
    [
        (0.10, 0, "emergency", 200_000_000),
        (0.05, 0, "emergency", 200_000_000),
        (0.12, 0, "protective", 200_000_000),
        (0.18, 0, "warning", 200_000_000),
        (0.50, 0, "normal", 200_000_000),
        (0.19, 1_000_000, "warning", 200_000_000),
        (0.50, 900_000_000, "warning", 900_000_000),
    ],
)
def test_disk_guard_modes_and_reserve_floor(
    free_fraction: float, one_batch_bytes: int, expected_mode: str, expected_reserve: int
) -> None:
    stats = VolumeStats(
        total_bytes=1_000_000_000, free_bytes=int(1_000_000_000 * free_fraction), one_batch_bytes=one_batch_bytes
    )
    state = evaluate_disk_state(stats)
    assert state.mode == expected_mode and state.reserve_bytes == expected_reserve


def test_disk_guard_rejects_free_bytes_above_total() -> None:
    with pytest.raises(ValueError, match="free_bytes"):
        VolumeStats(total_bytes=100, free_bytes=200, one_batch_bytes=0)


def test_retention_recommendation_bounds_and_validation() -> None:
    low = recommend_retention(ServiceProfile(1_000, 500.0), StorageProfile(1 << 40))
    high = recommend_retention(ServiceProfile(1_000_000, 500.0), StorageProfile(1 << 40))
    assert low.hot_days > high.hot_days
    assert recommend_retention(ServiceProfile(1, 500.0), StorageProfile(1 << 40)).hot_days == 1.0
    assert recommend_retention(ServiceProfile(1, 500.0), StorageProfile(1 << 50)).warm_days == 30.0
    constrained = recommend_retention(ServiceProfile(1_000_000, 500.0), StorageProfile(1 << 30))
    assert constrained.hot_days >= 0.0 and constrained.warm_days >= constrained.hot_days
    with pytest.raises(ValueError, match="events_per_second"):
        recommend_retention(ServiceProfile(0, 500.0), StorageProfile(1 << 40))


_PATH = "/api/admin/high-scale/retention-recommendation"


def _cfg(retention_days: int | None = None) -> dict[str, Any]:
    cfg: dict[str, Any] = {"service_id": "svc-hs", "provisioning": {"cron_sync": {}}}
    if retention_days is not None:
        cfg["provisioning"]["cron_sync"]["data_retention_days"] = retention_days
    return cfg


def _get(client: Any, cfg: dict[str, Any] | None, **params: Any) -> Any:
    query = {"service_id": "svc-hs", "events_per_second": 1000, **params}
    with (
        patch("backend.config.is_high_scale_mode", return_value=True),
        patch("backend.config.load_config", return_value=cfg),
    ):
        return client.get(_PATH, params=query)


def test_recommendation_endpoint_matches_pure_function_and_overrides(client: Any) -> None:
    resp = _get(client, _cfg(), total_bytes=_TB, average_row_bytes=800)
    assert resp.status_code == 200
    body = resp.json()
    expected = recommend_retention(ServiceProfile(1000, 800), StorageProfile(_TB))
    assert body["recommended_hot_days"] == pytest.approx(expected.hot_days)
    assert body["recommended_warm_days"] == pytest.approx(expected.warm_days)
    assert body["reasoning"] == expected.reasoning and body["volume_total_source"] == "request"
    assert body["source"] == "recommendation" and body["effective_retention_days"] == pytest.approx(expected.warm_days)

    over7 = _get(client, _cfg(retention_days=7), total_bytes=_TB).json()
    assert (over7["source"], over7["explicit_retention_days"], over7["effective_retention_days"]) == (
        "override",
        7,
        7.0,
    )
    assert over7["recommended_warm_days"] > 0

    over0 = _get(client, _cfg(retention_days=0), total_bytes=_TB).json()
    assert (over0["source"], over0["explicit_retention_days"], over0["effective_retention_days"]) == (
        "override",
        0,
        None,
    )


def test_recommendation_endpoint_sampled_volume_and_error_cases(client: Any) -> None:
    assert _get(client, _cfg()).status_code == 409
    assert _get(client, _cfg()).json()["detail"]["error"] == "high_scale_volume_unknown"

    class _Probe:
        def storage_health(self) -> dict:
            return {"disk_total": 2 * _TB, "disk_free": _TB}

    disk_sampler.sample_disk_state(_Probe())
    sampled = _get(client, _cfg()).json()
    assert sampled["volume_total_bytes"] == 2 * _TB and sampled["volume_total_source"] == "sampled"

    with patch("backend.config.is_high_scale_mode", return_value=False):
        r_disabled = client.get(_PATH, params={"service_id": "svc-hs", "events_per_second": 1000, "total_bytes": _TB})
    assert r_disabled.status_code == 404 and r_disabled.json()["detail"]["error"] == "high_scale_not_enabled"

    r_missing = _get(client, None, service_id="nope", events_per_second=1, total_bytes=_TB)
    assert r_missing.status_code == 404 and r_missing.json()["detail"]["error"] == "service_not_found"

    assert _get(client, _cfg(), total_bytes=_TB, events_per_second=0).status_code == 422


def test_recommendation_is_logged_and_never_writes_config(client: Any, caplog: pytest.LogCaptureFixture) -> None:
    assert _is_blocked_path(_PATH) is True
    with (
        patch("backend.config.is_high_scale_mode", return_value=True),
        patch("backend.config.load_config", return_value=_cfg()),
        patch("backend.config.save_config") as save,
    ):
        resp = client.get(_PATH, params={"service_id": "svc-hs", "events_per_second": 1000, "total_bytes": _TB})
    assert resp.status_code == 200
    save.assert_not_called()

    processors = structlog.get_config()["processors"]
    caplog.clear()
    try:
        structlog.configure(processors=list(processors))
        with caplog.at_level(logging.INFO):
            assert _get(client, _cfg(retention_days=14), total_bytes=_TB).status_code == 200
    finally:
        structlog.configure(processors=processors)

    events = [
        record.msg
        for record in caplog.records
        if isinstance(record.msg, dict) and record.msg.get("event") == "high_scale.retention_recommended"
    ]
    assert len(events) == 1
    ev = events[0]
    assert (ev["service_id"], ev["source"], ev["explicit_retention_days"]) == ("svc-hs", "override", 14)
    assert (ev["events_per_second"], ev["average_row_bytes"], ev["volume_total_bytes"], ev["volume_total_source"]) == (
        1000,
        500,
        _TB,
        "request",
    )
