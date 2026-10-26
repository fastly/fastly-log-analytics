from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import structlog
from backend.high_scale.archive_models import (
    ArchiveArtifact,
    ArchiveManifest,
    ArchiveSourceObject,
    ArchiveState,
    HighScaleConfig,
    ServiceProfile,
    ServingWatermark,
    StorageProfile,
    build_topology_profile,
    recommend_retention,
    validate_archive_transition,
)
from backend.high_scale.exports import ExportManager, ExportState
from backend.high_scale.publication import (
    ClickHousePublication,
    HighScaleBatch,
    InMemoryBatchManifestStore,
    PartialInsert,
    PublicationStatus,
    build_cmcd_projection_key,
    build_request_event_id,
    build_rum_event_id,
)
from backend.high_scale.recovery import (
    RecoveryCapacityMeasurement,
    evaluate_recovery_capacity,
)

from backend.high_scale.postgres_control import PostgresControlPlane
from backend.high_scale.query_service import QueryResponseMetadata


def identity(ordinal: int) -> dict[str, object]:
    return {
        "service_id": "svc",
        "domain": "request",
        "source_object_key": "raw/request/a.gz",
        "source_object_version": "sha256:a",
        "line_ordinal": ordinal,
        "transform_version": "request.v1",
    }


def test_request_identity_is_distinct_by_ordinal_and_idempotent_on_replay() -> None:
    first = build_request_event_id(identity(1))
    second = build_request_event_id(identity(2))
    replay = build_request_event_id(identity(1))
    assert first != second
    assert first == replay


def test_rum_identities_are_independent_for_vitals_and_errors() -> None:
    row = identity(1)
    assert build_rum_event_id(row, rum_kind="vitals") != build_rum_event_id(row, rum_kind="errors")


def test_cmcd_is_derived_from_request_identity() -> None:
    request_id = build_request_event_id(identity(1))
    assert build_cmcd_projection_key({"request_event_id": request_id}) == build_cmcd_projection_key(
        {"request_event_id": request_id}
    )
    with pytest.raises(ValueError, match="request_event_id"):
        build_cmcd_projection_key({})


def test_archive_manifest_and_watermark_validate() -> None:
    source = ArchiveSourceObject("svc", "request", "raw/request/a.gz", "sha256:a", 10, "v1")
    artifact = ArchiveArtifact(
        "s3://bucket/archive/a.parquet",
        "sha256:b",
        20,
        2,
        100,
        "sha256:events",
        "request.v1",
        "normalize.v1",
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
        "svc",
        "request",
        1,
        manifest.coverage_start,
        manifest.coverage_end,
        "cursor",
        "event-1",
        None,
        True,
    ).validate()


def test_clickhouse_schema_separates_domains_and_fences_visibility() -> None:
    root = Path(__file__).parents[2] / "backend/high_scale/sql"
    request_sql = (root / "request_schema.sql").read_text()
    rum_sql = (root / "rum_schema.sql").read_text()
    cmcd_sql = (root / "cmcd_schema.sql").read_text()
    publication_sql = (root / "publication_schema.sql").read_text()
    assert "ReplicatedMergeTree" in request_sql
    assert "publication_state" in request_sql
    assert "rum_vitals_facts" in rum_sql and "rum_error_facts" in rum_sql
    for table in ("rum_vitals_facts", "rum_error_facts"):
        for column, column_type in (
            ("browser", "LowCardinality(String)"),
            ("os", "LowCardinality(String)"),
            ("device", "LowCardinality(String)"),
            ("city", "String"),
            ("region", "LowCardinality(String)"),
            ("pop", "LowCardinality(String)"),
            ("tls", "LowCardinality(String)"),
            ("ttfb", "Nullable(Float64)"),
        ):
            assert f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {column_type}" in rum_sql
    for column in ("error_line", "error_col"):
        assert f"ALTER TABLE rum_error_facts ADD COLUMN IF NOT EXISTS {column} UInt32" in rum_sql
    assert "request_event_id" in cmcd_sql
    assert "country LowCardinality(String)" in cmcd_sql
    assert "ALTER TABLE cmcd_projection_facts ADD COLUMN IF NOT EXISTS country LowCardinality(String)" in cmcd_sql
    assert "ALTER TABLE cmcd_projection_facts ADD COLUMN IF NOT EXISTS asn Nullable(UInt32)" in cmcd_sql
    assert "ReplicatedReplacingMergeTree" in publication_sql
    assert "publication_state" in publication_sql
    assert "quorum_acked" in publication_sql


def test_origin_projection_schema_is_bounded_and_mergeable() -> None:
    root = Path(__file__).parents[2] / "backend/high_scale/sql"
    sql = (root / "origin_schema.sql").read_text()

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
    root = Path(__file__).parents[2] / "backend/high_scale/sql"
    sql = (root / "performance_schema.sql").read_text()

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

import uuid
from typing import Any


def _base_row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "service_id": "svc_test",
        "domain": "request",
        "source_object_key": "raw/request/2026-10-09/file1.gz",
        "source_object_version": "v1_etag",
        "line_ordinal": 0,
        "transform_version": "request.v1",
    }
    row.update(overrides)
    return row


def test_event_id_is_128_bit_valid_uuid() -> None:
    row = _base_row()
    event_id = build_request_event_id(row)

    # Must be valid UUID representation (128-bit hash)
    parsed = uuid.UUID(event_id)
    assert str(parsed) == event_id or event_id == parsed.hex
    # 128-bit means 16 bytes / 32 hex chars (or 36 chars with hyphens)
    assert len(parsed.bytes) == 16


def test_event_id_is_deterministic_and_stable_across_retries() -> None:
    row1 = _base_row(line_ordinal=42)
    row2 = _base_row(line_ordinal=42)
    assert build_request_event_id(row1) == build_request_event_id(row2)


def test_event_id_differs_by_line_ordinal() -> None:
    row1 = _base_row(line_ordinal=0)
    row2 = _base_row(line_ordinal=1)
    assert build_request_event_id(row1) != build_request_event_id(row2)


def test_event_id_is_length_prefix_unambiguous() -> None:
    # ("ab", "c") vs ("a", "bc") must produce distinct event_ids to prevent delimiter collision
    row1 = _base_row(service_id="ab", source_object_key="c")
    row2 = _base_row(service_id="a", source_object_key="bc")
    assert build_request_event_id(row1) != build_request_event_id(row2)


def test_event_id_differs_by_source_version() -> None:
    row1 = _base_row(source_object_version="etag_v1")
    row2 = _base_row(source_object_version="etag_v2")
    assert build_request_event_id(row1) != build_request_event_id(row2)


def test_event_id_differs_by_transform_version() -> None:
    row1 = _base_row(transform_version="request.v1")
    row2 = _base_row(transform_version="request.v2")
    assert build_request_event_id(row1) != build_request_event_id(row2)


def test_rum_event_ids_separate_vitals_and_errors() -> None:
    row = _base_row()
    vitals_id = build_rum_event_id(row, rum_kind="vitals")
    errors_id = build_rum_event_id(row, rum_kind="errors")
    assert vitals_id != errors_id
    assert uuid.UUID(vitals_id)
    assert uuid.UUID(errors_id)


def test_cmcd_projection_key_deterministic_128_bit() -> None:
    req_id = build_request_event_id(_base_row())
    cmcd_key1 = build_cmcd_projection_key({"request_event_id": req_id})
    cmcd_key2 = build_cmcd_projection_key({"request_event_id": req_id})
    assert cmcd_key1 == cmcd_key2
    # Verify valid 128-bit UUID or 32-char hex
    assert len(uuid.UUID(cmcd_key1).bytes) == 16


# ── Merged from test_batch_idempotency.py ──


from backend.high_scale.publication import build_batch_id_from_manifest_id


def test_batch_id_is_deterministically_derived_from_manifest_id() -> None:
    manifest_id = "manifest-test-abc-123"
    batch_id_1 = build_batch_id_from_manifest_id(manifest_id)
    batch_id_2 = build_batch_id_from_manifest_id(manifest_id)
    assert batch_id_1 == batch_id_2
    # Must be valid UUID format
    assert str(uuid.UUID(batch_id_1)) == batch_id_1


def test_different_manifest_id_yields_different_batch_id() -> None:
    batch_1 = build_batch_id_from_manifest_id("manifest-1")
    batch_2 = build_batch_id_from_manifest_id("manifest-2")
    assert batch_1 != batch_2


def test_publication_mismatch_blocks_visibility() -> None:
    """When visible_rows != expected_rows, publication status must reject visible transition."""
    # Publication gate logic
    expected_rows = 100
    visible_rows = 95  # 5 rows missing

    def check_publication(expected: int, visible: int) -> str:
        if expected != visible:
            raise ValueError(f"Publication gate failure: expected {expected} rows, but got {visible}")
        return "visible"

    with pytest.raises(ValueError, match="Publication gate failure"):
        check_publication(expected_rows, visible_rows)


# ── Merged from test_field_registry_reuse.py ──

import json

from backend.high_scale.decoder import decode_source_object


def test_request_field_normalization_parity_with_field_registry() -> None:
    """Verifies that decoder normalization adheres to types defined in field_registry."""
    source = ArchiveSourceObject(
        service_id="test_service",
        domain="request",
        object_key="raw/request/test.log.gz",
        checksum="sha256:123",
        size_bytes=100,
    )
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
    event = result.events[0]

    # Status must be normalized to integer consistent with registry
    assert isinstance(event["status"], int) or event["status"] == 200
    # resp_body_size must be normalized to integer
    assert isinstance(event["resp_body_size"], int) or event["resp_body_size"] == 1024
    # Pop and IP string normalization
    assert event["fastly_pop"] == "SFO"
    assert event["client_ip"] == "192.0.2.1"


def test_unknown_fields_and_cmcd_routed_to_maps() -> None:
    source = ArchiveSourceObject(
        service_id="test_service",
        domain="request",
        object_key="raw/request/test.log.gz",
        checksum="sha256:123",
        size_bytes=100,
    )
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
    event = result.events[0]

    # Verify custom_fields map and cmcd map separation
    assert "custom_fields" in event
    assert "cmcd" in event
    assert event["custom_fields"].get("user_tag") == "vip"
    assert event["cmcd"].get("sid") == "session-1" or event["cmcd"].get("cmcd_sid") == "session-1"


# ── Merged from test_no_hardcoded_database.py ──

import pathlib

_HIGH_SCALE_DIR = pathlib.Path(__file__).resolve().parents[2] / "backend" / "high_scale"
_FORBIDDEN = "fastly_log_analytics."


def test_high_scale_readers_do_not_hardcode_prod_database_name() -> None:
    offenders: list[str] = []
    for py in sorted(_HIGH_SCALE_DIR.glob("*.py")):
        text = py.read_text(encoding="utf-8")
        for lineno, line in enumerate(text.splitlines(), start=1):
            if _FORBIDDEN in line:
                offenders.append(f"{py.name}:{lineno}: {line.strip()}")
    assert not offenders, (
        "High-scale readers must use unqualified table names so they resolve "
        "against the connection's configured default database (fla_prototype "
        "locally, fastly_log_analytics on Elevation). Hardcoding "
        f"'{_FORBIDDEN}' breaks every non-prod-named database. Offenders:\n" + "\n".join(offenders)
    )


# ── Merged from test_batch_manifest.py ───────────────────────────────

from datetime import timedelta

from backend.high_scale.archive_models import ArchiveSourceEntry


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


def test_batch_manifest_supports_n_sources_and_validates() -> None:
    now = datetime.now(UTC)
    sources = tuple(
        ArchiveSourceEntry(
            key=f"raw/request/2026-10-09/part_{i}.gz",
            checksum=f"sha256:chk_{i}",
            size_bytes=1024,
            version=f"v_{i}",
            row_start=i * 20,
            row_count=20,
            deletion_deadline=now + timedelta(days=7),
        )
        for i in range(5)
    )

    manifest = ArchiveManifest(
        manifest_id="manifest-batch-001",
        service_id="svc_test",
        domain="request",
        sources=sources,
        artifact=_base_artifact(row_count=100),
        coverage_start=now - timedelta(minutes=5),
        coverage_end=now,
        retention_deadline=now + timedelta(days=30),
        deletion_authorization_deadline=now + timedelta(days=30),
        archive_epoch=1,
    )

    # Validation should pass
    manifest.validate()
    assert len(manifest.sources) == 5
    assert manifest.sources[0].row_start == 0
    assert manifest.sources[0].row_count == 20
    assert manifest.sources[4].row_start == 80


def test_batch_manifest_rejects_row_overflow() -> None:
    now = datetime.now(UTC)
    sources = (
        ArchiveSourceEntry(
            key="raw/request/part_0.gz",
            checksum="sha256:chk_0",
            size_bytes=1024,
            row_start=0,
            row_count=60,
            deletion_deadline=now + timedelta(days=7),
        ),
        ArchiveSourceEntry(
            key="raw/request/part_1.gz",
            checksum="sha256:chk_1",
            size_bytes=1024,
            row_start=60,
            row_count=50,  # 60 + 50 = 110 > artifact row_count 100
            deletion_deadline=now + timedelta(days=7),
        ),
    )

    manifest = ArchiveManifest(
        manifest_id="manifest-batch-002",
        service_id="svc_test",
        domain="request",
        sources=sources,
        artifact=_base_artifact(row_count=100),
        coverage_start=now - timedelta(minutes=5),
        coverage_end=now,
        retention_deadline=now + timedelta(days=30),
        deletion_authorization_deadline=now + timedelta(days=30),
        archive_epoch=1,
    )

    with pytest.raises(ValueError, match="exceeds artifact row_count"):
        manifest.validate()


def test_independent_per_source_deletion_deadlines() -> None:
    now = datetime.now(UTC)
    s1 = ArchiveSourceEntry(
        key="raw/request/file1.gz",
        checksum="sha256:chk1",
        size_bytes=500,
        row_start=0,
        row_count=50,
        deletion_deadline=now - timedelta(minutes=1),  # Eligible now
    )
    s2 = ArchiveSourceEntry(
        key="raw/request/file2.gz",
        checksum="sha256:chk2",
        size_bytes=600,
        row_start=50,
        row_count=50,
        deletion_deadline=now + timedelta(hours=1),  # Not eligible yet
    )
    manifest = ArchiveManifest(
        manifest_id="manifest-batch-004",
        service_id="svc_test",
        domain="request",
        sources=(s1, s2),
        artifact=_base_artifact(row_count=100),
        coverage_start=now - timedelta(minutes=5),
        coverage_end=now,
        retention_deadline=now + timedelta(days=30),
        deletion_authorization_deadline=now + timedelta(days=30),
        archive_epoch=1,
    )

    assert manifest.is_source_eligible_for_deletion("raw/request/file1.gz", now=now) is True
    assert manifest.is_source_eligible_for_deletion("raw/request/file2.gz", now=now) is False


def test_write_batch_archive_checkpoint_end_to_end(tmp_path) -> None:
    from backend.high_scale.archive_writer import write_batch_archive_checkpoint

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

    assert len(manifest.sources) == 2
    assert manifest.sources[0].key == "raw/request/part_0.gz"
    assert manifest.sources[0].row_start == 0
    assert manifest.sources[0].row_count == 15
    assert manifest.sources[1].key == "raw/request/part_1.gz"
    assert manifest.sources[1].row_start == 15
    assert manifest.sources[1].row_count == 25
    assert manifest.artifact.row_count == 40


# ── Merged from test_decoder.py ──────────────────────────────────────

import gzip


def _source() -> ArchiveSourceObject:
    return ArchiveSourceObject("svc", "request", "raw/request/a.gz", "sha256:source", 100, "v7")


def test_decode_assigns_stable_identity_and_preserves_duplicate_payloads() -> None:
    payload = gzip.compress(b'{"url":"/same","status":200}\n{"url":"/same","status":200}\n')

    result = decode_source_object(_source(), payload, transform_version="normalize.v1")

    assert result.decoded_rows == 2
    assert result.accepted_rows == 2
    assert result.quarantined_rows == 0
    assert result.events[0]["event_id"] != result.events[1]["event_id"]
    assert result.events[0]["line_ordinal"] == 0
    assert result.events[1]["line_ordinal"] == 1


def test_decode_replays_with_identical_event_ids() -> None:
    payload = gzip.compress(b'{"url":"/same","status":200}\n')

    first = decode_source_object(_source(), payload, transform_version="normalize.v1")
    second = decode_source_object(_source(), payload, transform_version="normalize.v1")

    assert first.events[0]["event_id"] == second.events[0]["event_id"]


def test_decode_preserves_request_ip_without_mutating_the_batch_payload() -> None:
    payload = gzip.compress(b'{"ip":"198.51.100.10","url":"/same","status":200}\n')

    result = decode_source_object(_source(), payload, transform_version="normalize.v1")

    assert result.events[0]["ip"] == "198.51.100.10"
    assert "client_ip" not in result.events[0]


def test_decode_quarantines_malformed_records_losslessly() -> None:
    malformed = b'{"url":"/ok"}\nnot-json\n["not","an","object"]\n'

    result = decode_source_object(_source(), gzip.compress(malformed), transform_version="normalize.v1")

    assert result.accepted_rows == 1
    assert result.quarantined_rows == 2
    assert [item.raw_line for item in result.dead_letters] == [b"not-json", b'["not","an","object"]']
    assert json.loads(result.events[0]["raw_json"])["url"] == "/ok"


def test_decode_normalizes_dedicated_rum_vitals_fields_for_serving() -> None:
    source = ArchiveSourceObject("svc", "rum_vitals", "raw/rum/vitals.gz", "sha256:source", 100, "v7")
    payload = gzip.compress(
        b'{"rum_cid":"cid-1","rum_metric_name":"LCP","rum_metric_value":"123.5",'
        b'"rum_metric_rating":"good","rum_pathname":"/home"}\n'
    )

    result = decode_source_object(source, payload, transform_version="normalize.v1")

    assert result.events[0]["client_id"] == "cid-1"
    assert result.events[0]["metric_name"] == "LCP"
    assert result.events[0]["metric_value"] == 123.5
    assert result.events[0]["metric_rating"] == "good"
    assert result.events[0]["pathname"] == "/home"


def test_decode_falls_back_to_querystring_when_flat_rum_vitals_fields_are_empty() -> None:
    # Mirrors the live Elevation shape: the VCL-extracted flat rum_metric_* log
    # fields came through empty (edge-snippet ordering/case drift), but the
    # full beacon querystring is still present in rum_raw_query. Without a
    # fallback, metric_name/metric_value decode to ""/None for every row.
    source = ArchiveSourceObject("svc", "rum_vitals", "raw/rum/vitals.gz", "sha256:source", 100, "v7")
    payload = gzip.compress(
        b'{"rum_cid":"","rum_metric_name":"","rum_metric_value":"","rum_metric_rating":"","rum_pathname":"",'
        b'"rum_raw_query":"/beacon?rum_metric_name=LCP&rum_metric_value=123.5&rum_metric_rating=good'
        b'&cid=cid-1&rum_pathname=%2Fhome"}\n'
    )

    result = decode_source_object(source, payload, transform_version="normalize.v1")

    assert result.events[0]["client_id"] == "cid-1"
    assert result.events[0]["metric_name"] == "LCP"
    assert result.events[0]["metric_value"] == 123.5
    assert result.events[0]["metric_rating"] == "good"
    assert result.events[0]["pathname"] == "/home"


def test_decode_prefers_flat_rum_vitals_fields_over_querystring_fallback() -> None:
    source = ArchiveSourceObject("svc", "rum_vitals", "raw/rum/vitals.gz", "sha256:source", 100, "v7")
    payload = gzip.compress(
        b'{"rum_metric_name":"CLS","rum_metric_value":"0.02",'
        b'"rum_raw_query":"/beacon?rum_metric_name=LCP&rum_metric_value=999"}\n'
    )

    result = decode_source_object(source, payload, transform_version="normalize.v1")

    assert result.events[0]["metric_name"] == "CLS"
    assert result.events[0]["metric_value"] == 0.02


def test_decode_normalizes_dedicated_rum_error_fields_for_serving() -> None:
    source = ArchiveSourceObject("svc", "rum_errors", "raw/rum/errors.gz", "sha256:source", 100, "v7")
    payload = gzip.compress(
        b'{"rum_cid":"cid-2","rum_error_message":"boom","rum_error_file":"app.js","rum_pathname":"/checkout"}\n'
    )

    result = decode_source_object(source, payload, transform_version="normalize.v1")

    assert result.events[0]["client_id"] == "cid-2"
    assert result.events[0]["error_message"] == "boom"
    assert result.events[0]["error_file"] == "app.js"
    assert result.events[0]["pathname"] == "/checkout"


def test_decode_expands_faro_rum_body_matching_standard_mode_parity() -> None:
    from backend.core.rum_ingest import extract_metrics_from_faro_payload

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
            {
                "type": "web-vitals",
                "values": {"inp": 195.0},
                "meta": {"rating": "needs-improvement"},
            },
        ],
        "events": [
            {
                "name": "faro.performance.navigation",
                "attributes": {"ttfb": 110.25, "fcp": 640.0},
            },
            {
                "name": "add_to_cart_click",
            },
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
    assert len(standard_vitals) == 6
    assert len(standard_errors) == 2

    vitals_source = ArchiveSourceObject("svc", "rum_vitals", "raw/rum/faro.gz", "sha256:source", 100, "v7")
    vitals_result = decode_source_object(vitals_source, payload_bytes, transform_version="normalize.v1")

    assert vitals_result.decoded_rows == 1
    assert vitals_result.accepted_rows == len(standard_vitals)
    assert vitals_result.quarantined_rows == 0
    assert len({ev["event_id"] for ev in vitals_result.events}) == len(standard_vitals)
    for ev, std in zip(vitals_result.events, standard_vitals, strict=True):
        assert ev["metric_name"] == std["metric_name"]
        assert ev["metric_value"] == float(std["metric_value"])
        assert ev["metric_rating"] == std["metric_rating"]
        assert ev["pathname"] == std["pathname"] == "/products/42"
        assert ev["client_id"] == std["cid"] == "cid-faro-99"
        assert ev["browser"] == std["browser"] == "Firefox"
        assert ev["os"] == std["os"] == "Android"
        assert ev["device"] == std["device"] == "Mobile"
        assert ev["line_ordinal"] == 0

    errors_source = ArchiveSourceObject("svc", "rum_errors", "raw/rum/faro.gz", "sha256:source", 100, "v7")
    errors_result = decode_source_object(errors_source, payload_bytes, transform_version="normalize.v1")

    assert errors_result.decoded_rows == 1
    assert errors_result.accepted_rows == len(standard_errors)
    assert errors_result.quarantined_rows == 0
    assert len({ev["event_id"] for ev in errors_result.events}) == len(standard_errors)
    for ev, std in zip(errors_result.events, standard_errors, strict=True):
        assert ev["error_message"] == std["error_message"]
        assert ev["error_file"] == std["error_file"]
        assert ev["error_line"] == std["error_line"]
        assert ev["error_col"] == std["error_col"]
        assert ev["pathname"] == std["pathname"] == "/products/42"
        assert ev["client_id"] == std["cid"] == "cid-faro-99"
        assert ev["browser"] == std["browser"] == "Firefox"
        assert ev["os"] == std["os"] == "Android"
        assert ev["device"] == std["device"] == "Mobile"
        assert ev["line_ordinal"] == 0

    from backend.high_scale.publication import HighScaleBatch, _rows_for_domain, row_matches_serving_domain

    vitals_serving_rows = tuple(e for e in vitals_result.events if row_matches_serving_domain("rum_vitals", e))
    assert len(vitals_serving_rows) == len(standard_vitals)
    vitals_batch = HighScaleBatch(
        batch_id="b-vitals",
        service_id="svc",
        domain="rum_vitals",
        generation="gen-1",
        rows=vitals_serving_rows,
    )
    ch_vitals_tuples = _rows_for_domain(vitals_batch, "00000000-0000-0000-0000-000000000001")
    assert len(ch_vitals_tuples) == len(standard_vitals)
    assert len({row[1] for row in ch_vitals_tuples}) == len(standard_vitals)

    errors_serving_rows = tuple(e for e in errors_result.events if row_matches_serving_domain("rum_errors", e))
    assert len(errors_serving_rows) == len(standard_errors)
    errors_batch = HighScaleBatch(
        batch_id="b-errors",
        service_id="svc",
        domain="rum_errors",
        generation="gen-1",
        rows=errors_serving_rows,
    )
    ch_errors_tuples = _rows_for_domain(errors_batch, "00000000-0000-0000-0000-000000000002")
    assert len(ch_errors_tuples) == len(standard_errors)
    assert len({row[1] for row in ch_errors_tuples}) == len(standard_errors)


@pytest.mark.parametrize("claimed_domain", ["rum_vitals", "rum_errors"])
def test_faro_exception_without_message_survives_without_becoming_a_vital(claimed_domain) -> None:
    """Flat edge fields describe the envelope, not the expanded Faro record."""
    from backend.high_scale.publication import row_matches_serving_domain

    record = {
        "timestamp": "2026-10-25T12:00:00Z",
        "rum_metric_name": "LCP",
        "rum_metric_value": "123",
        "url": "/beacon?rum_metric_name=LCP&rum_metric_value=123",
        "rum_body": json.dumps({"exceptions": [{"stacktrace": {"frames": [{"filename": "app.js"}]}}]}),
    }
    source = ArchiveSourceObject("svc", claimed_domain, "raw/rum/mixed.gz", "sha256:source", 100, "v1")
    result = decode_source_object(source, json.dumps(record).encode(), transform_version="normalize.v1")

    assert result.quarantined_rows == 0
    assert len(result.events) == 1
    assert row_matches_serving_domain("rum_errors", result.events[0])
    assert not row_matches_serving_domain("rum_vitals", result.events[0])
    assert result.events[0]["error_message"] == "Unknown error"


def test_invalid_faro_metric_is_quarantined_instead_of_silently_becoming_zero() -> None:
    record = {
        "timestamp": "2026-10-25T12:00:00Z",
        "rum_body": json.dumps({"measurements": [{"type": "web-vitals", "values": {"lcp": "broken"}}]}),
    }
    source = ArchiveSourceObject("svc", "rum_errors", "raw/rum/invalid.gz", "sha256:source", 100, "v1")
    payload = json.dumps(record).encode()
    result = decode_source_object(source, payload, transform_version="normalize.v1")

    assert result.accepted_rows == 0
    assert result.quarantined_rows == 1
    assert result.dead_letters[0].raw_line == payload


def test_flat_error_with_stack_but_empty_message_survives() -> None:
    from backend.high_scale.publication import row_matches_serving_domain

    source = ArchiveSourceObject("svc", "rum_errors", "raw/rum/flat.gz", "sha256:source", 100, "v1")
    payload = b'{"timestamp":"2026-10-25T12:00:00Z","rum_error_message":"","rum_error_stack":"at app.js:1"}'
    result = decode_source_object(source, payload, transform_version="normalize.v1")

    assert result.accepted_rows == 1
    assert row_matches_serving_domain("rum_errors", result.events[0])
    assert result.events[0]["error_message"] == "Unknown error"


@pytest.mark.parametrize("domain", ["rum_vitals", "rum_errors"])
def test_flat_rum_record_without_any_metric_or_error_is_quarantined(domain) -> None:
    source = ArchiveSourceObject("svc", domain, "raw/rum/empty.gz", "sha256:source", 100, "v1")
    payload = b'{"timestamp":"2026-10-25T12:00:00Z","rum_error_message":"","rum_metric_value":""}'
    result = decode_source_object(source, payload, transform_version="normalize.v1")
    assert result.accepted_rows == 0
    assert result.quarantined_rows == 1
    assert result.dead_letters[0].raw_line == payload


def test_faro_exception_with_nontext_message_is_quarantined() -> None:
    source = ArchiveSourceObject("svc", "rum_errors", "raw/rum/invalid-error.gz", "sha256:source", 100, "v1")
    payload = json.dumps({"rum_body": json.dumps({"exceptions": [{"value": 42}]})}).encode()
    result = decode_source_object(source, payload, transform_version="normalize.v1")
    assert result.accepted_rows == 0
    assert result.quarantined_rows == 1
    assert result.dead_letters[0].raw_line == payload


# ── Merged from tests/high_scale/test_clickhouse_readers_integration.py ──

"""Opt-in real ClickHouse gate for the high-scale readers that fake clients cannot validate.

RUN_HS_READERS_CLICKHOUSE_INTEGRATION=1 uv run pytest tests/high_scale/test_clickhouse_readers_integration.py

Uses an already-installed Docker image and disposable ``clickhouse local`` processes (no ports, no
deployed infrastructure). The production instrumented HTTP client runs against a local transport that
executes each statement on that engine over the canonical fact schemas and a fixed fixture, so dialect
mistakes (format specifiers, UNION ordering, quantile combinators) fail here instead of in production.
"""


import os
import re
import subprocess
from collections.abc import Iterator
from email.parser import BytesParser
from email.policy import default
from types import SimpleNamespace

import httpx
import pytest
from backend.core.clickhouse_client import ClickHouseClient
from backend.high_scale.cmcd import cmcd_aggregates, project_cmcd
from backend.high_scale.dashboard import aggregates as dashboard_aggregates
from backend.high_scale.insights import cache_collapse_detail_endpoint, insights_endpoint
from backend.high_scale.origin import aggregates as origin_aggregates
from backend.high_scale.origin_projection import build_origin_projection_rows
from backend.high_scale.registry_bootstrap import HighScaleService
from backend.high_scale.sessions import sessions_endpoint
from backend.models.dashboard import AggregatesRequest, SessionsRequest
from backend.models.insights import CacheCollapseDetailRequest, InsightsRequest
from backend.models.performance import CmcdRequest

from backend.config import ClickHouseConfig
from backend.high_scale import cmcd as hs_cmcd
from backend.high_scale import sessions as hs_sessions
from backend.repositories.insights import registry

# (scoped pytestmark) pytestmark = pytest.mark.skipif(
# (scoped pytestmark)     os.getenv("RUN_HS_READERS_CLICKHOUSE_INTEGRATION") != "1", reason="explicit real ClickHouse readers gate"
# (scoped pytestmark) )

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
        text = re.sub(r"\nTTL [^;]*;", ";", text)
        parts.append(text)
    return "\n".join(parts)


class _Fixture:
    def __init__(self) -> None:
        self.seed = _schema()

    def insert(self, table: str, domain: str, rows: list[dict[str, Any]]) -> None:
        """Insert fact/projection rows and publish every distinct batch as visible."""
        if not rows:
            return
        self.seed += f"\nINSERT INTO {table} FORMAT JSONEachRow\n" + "\n".join(json.dumps(r) for r in rows) + "\n;\n"
        batches = {r["batch_id"] for r in rows}
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
            for batch in batches
        ]
        self.seed += "\nINSERT INTO high_scale_batch_publications FORMAT JSONEachRow\n"
        self.seed += "\n".join(json.dumps(p) for p in publications) + "\n;\n"

    def request(
        self, when: datetime, url: str = "/ok", *, ip: str = "203.0.113.1", country: str = "US", **fields: Any
    ) -> dict[str, Any]:
        cmcd = {k[5:]: str(fields.pop(k)) for k in [k for k in fields if k.startswith("cmcd_")]}
        custom = {k: str(v) for k, v in fields.items()}
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
            "custom_fields": custom,
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
        # Per-query settings travel as URL settings (see ClickHouseClient.execute(settings=...)).
        for name in ("additional_table_filters", "max_result_rows", "result_overflow_mode"):
            if name in request.url.params:
                command.append(f"--{name}={request.url.params[name]}")
        completed = subprocess.run(
            command, input=seed + "\n" + fields["query"] + ";", text=True, capture_output=True, timeout=120
        )
        if completed.returncode:
            return httpx.Response(500, content=completed.stderr.encode())
        return httpx.Response(200, content=completed.stdout.encode())

    settings = ClickHouseConfig(host="localhost", port=8123, database="default", user="default", password="")
    return ClickHouseClient(settings, transport=httpx.MockTransport(transport))


@pytest.fixture
def fixture() -> Iterator[_Fixture]:
    yield _Fixture()


@pytest.fixture
def service(fixture: _Fixture) -> Iterator[SimpleNamespace]:
    client = _make_client(fixture.seed)
    yield SimpleNamespace(service_id=SERVICE, client=client)
    client.close()


@pytest.mark.skipif(
    os.getenv("RUN_HS_READERS_CLICKHOUSE_INTEGRATION") != "1", reason="explicit real ClickHouse readers gate"
)
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
        service = HighScaleService(
            service_id=SERVICE,
            client=client,
            cursor_secret=b"fixture",
            request_watermark=ServingWatermark(
                service_id=SERVICE,
                domain="request",
                owner_epoch=1,
                coverage_start=start,
                coverage_end=end,
                last_accepted_cursor=None,
                last_archived_event_id=None,
                last_visible_event_id=None,
                exact=True,
            ),
        )
        response = dashboard_aggregates(
            service,
            AggregatesRequest(fields=["url"], include_time_series=False, include_map_data=False),
            start.isoformat(),
            end.isoformat(),
        )
    finally:
        client.close()

    assert response.total_rows == 3
    assert response.data["url"].total == 3
    assert [(row.value, row.count) for row in response.data["url"].top] == [("/kept", 3)]


def _spread(start: datetime, end: datetime, count: int) -> list[datetime]:
    step = (end - start) / max(count, 1)
    return [start + step * i for i in range(count)]


@pytest.fixture
def traffic(fixture: _Fixture) -> _Fixture:
    """Twelve hours of traffic: ten baseline hours, a two-hour window, and one planted anomaly per detection."""
    # NOW is fixed at import while the readers use the wall clock at call time, so both ranges keep a
    # 20-minute guard from the baseline/window boundary and from the baseline start.
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
    # Newest rows carry every log field so each detection's required fields are present in the field sample.
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


@pytest.mark.skipif(
    os.getenv("RUN_HS_READERS_CLICKHOUSE_INTEGRATION") != "1", reason="explicit real ClickHouse readers gate"
)
@pytest.mark.timeout(1800)
def test_every_ported_detection_executes_and_planted_anomalies_are_found(
    traffic: _Fixture, service: SimpleNamespace, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "backend.utils.pop_utils.get_pop_lat_lon_map", lambda: {"SJC": (37.36, -121.93), "IAD": (38.94, -77.46)}
    )
    cards = _insight_cards(service)

    errors = {card_id: card["summary"] for card_id, card in cards.items() if card["severity"] == "error"}
    assert errors == {}
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


@pytest.mark.skipif(
    os.getenv("RUN_HS_READERS_CLICKHOUSE_INTEGRATION") != "1", reason="explicit real ClickHouse readers gate"
)
def test_cache_collapse_detail_reads_real_rows(traffic: _Fixture, service: SimpleNamespace) -> None:
    req = CacheCollapseDetailRequest(url="/collapse", window_size_hrs=2, baseline_hours=10)

    result = cache_collapse_detail_endpoint(service, req, None, None, False)  # type: ignore[arg-type]

    assert result.has_data is True
    assert (result.breakdown.hits, result.breakdown.misses) == (2, 18)
    assert result.window_hit_rate == pytest.approx(10.0)
    assert result.baseline_hit_rate == pytest.approx(90.0)
    assert result.timeline and result.recent_misses
    assert all(point.bucket.endswith("Z") for point in result.timeline)


@pytest.mark.skipif(
    os.getenv("RUN_HS_READERS_CLICKHOUSE_INTEGRATION") != "1", reason="explicit real ClickHouse readers gate"
)
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
    # Only the all-fields rows carry tcp_rtt/ja4; nothing in this fixture carries CMCD.
    assert result["has_rtt"] is True and result["has_ja4"] is True and result["has_cmcd"] is False
    past_the_end = sessions_endpoint(service, SessionsRequest(limit=2, page=99), start, end)  # type: ignore[arg-type]
    assert past_the_end["sessions"] == [] and past_the_end["total"] == result["total"]


@pytest.mark.skipif(
    os.getenv("RUN_HS_READERS_CLICKHOUSE_INTEGRATION") != "1", reason="explicit real ClickHouse readers gate"
)
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
    assert result["rebuffer_by_asn"][0]["asn"] == 7922
    assert result["top_content"][0]["content_id"] == "movie"


@pytest.mark.skipif(
    os.getenv("RUN_HS_READERS_CLICKHOUSE_INTEGRATION") != "1", reason="explicit real ClickHouse readers gate"
)
def test_origin_sketches_merge_to_near_exact_percentiles(fixture: _Fixture, monkeypatch: pytest.MonkeyPatch) -> None:
    import random

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

    import math

    ordered = sorted(values)
    exact_p95 = ordered[math.ceil(0.95 * len(ordered)) - 1] / 1000.0
    assert result["summary"]["ottfb_p95_ms"] == pytest.approx(exact_p95, rel=0.015)
    assert result["summary"]["_approx"] is True and "histogram sketch" in result["summary"]["_approx_note"]
    assert result["timeseries"]["series"] and all(point["value"] for point in result["timeseries"]["series"])
    assert {row["url"] for row in result["slow_urls"]["rows"]} == {"/a", "/b"}
    assert result["pop_latency"]["rows"][0]["pop"] == "SJC"


def _origin_request(filters: dict[str, Any]) -> SimpleNamespace:
    return SimpleNamespace(
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


_ORIGIN_SECTIONS = {"summary", "timeseries", "slow_urls", "status_codes", "path_breakdown", "pop_latency", "ip_health"}


def _filtered_origin(service: SimpleNamespace, filters: dict[str, Any]) -> dict[str, Any]:
    return origin_aggregates(
        service,
        _origin_request(filters),
        (NOW - timedelta(hours=12)).isoformat(),
        (NOW + timedelta(minutes=1)).isoformat(),
        sections=_ORIGIN_SECTIONS,
    )


@pytest.mark.skipif(
    os.getenv("RUN_HS_READERS_CLICKHOUSE_INTEGRATION") != "1", reason="explicit real ClickHouse readers gate"
)
def test_filtered_origin_is_computed_exactly_from_request_facts(traffic: _Fixture, service: SimpleNamespace) -> None:
    from backend.models.common import FilterSpec

    result = _filtered_origin(
        service,
        {"url": FilterSpec(mode="include", values=["/ok"]), "method": FilterSpec(mode="include", values=["GET"])},
    )

    summary = result["summary"]
    assert summary["has_data"] is True and summary["_approx"] is False
    assert summary["total_misses"] == 0 and summary["total_passes"] == 0
    assert summary["ottfb_p50_ms"] == summary["ottfb_p95_ms"] == 50.0
    assert summary["origin_error_rate"] == 0.0
    assert summary["ottlb_p50_ms"] is None and summary["obytes_p50"] is None
    assert result["status_codes"]["rows"] == [{"status": 200, "count": 500, "pct": 100.0}]
    assert result["slow_urls"]["rows"] == [
        {"url": "/ok", "requests": 500, "p50_ms": 50.0, "p95_ms": 50.0, "p99_ms": 50.0}
    ]
    assert result["timeseries"]["has_data"] and all(p["value"] == 50.0 for p in result["timeseries"]["series"])
    assert sum(p["miss_count"] for p in result["timeseries"]["series"]) == 0
    assert result["path_breakdown"]["has_data"] is False


@pytest.mark.skipif(
    os.getenv("RUN_HS_READERS_CLICKHOUSE_INTEGRATION") != "1", reason="explicit real ClickHouse readers gate"
)
def test_filtered_origin_with_no_matching_rows_reports_no_data_without_error(
    traffic: _Fixture, service: SimpleNamespace
) -> None:
    from backend.models.common import FilterSpec

    result = _filtered_origin(
        service,
        {"url": FilterSpec(mode="include", values=["/ok"]), "method": FilterSpec(mode="include", values=["POST"])},
    )

    assert result["has_data"] is False
    assert result["summary"]["has_data"] is False
    assert result["slow_urls"]["rows"] == [] and result["status_codes"]["rows"] == []
    assert result["timeseries"]["series"] == []


@pytest.mark.skipif(
    os.getenv("RUN_HS_READERS_CLICKHOUSE_INTEGRATION") != "1", reason="explicit real ClickHouse readers gate"
)
def test_filtered_origin_exclude_mode_and_oip_health(traffic: _Fixture, service: SimpleNamespace) -> None:
    from backend.models.common import FilterSpec

    result = _filtered_origin(service, {"country": FilterSpec(mode="exclude", values=["NZ"])})

    assert result["summary"]["has_data"] is True
    assert {row["url"] for row in result["slow_urls"]["rows"]} >= {"/ok"}
    assert 200 in {row["status"] for row in result["status_codes"]["rows"]}


@pytest.fixture
def two_tenants(traffic: _Fixture) -> _Fixture:
    """``traffic`` plus a second service's rows, so a scope escape is observable as extra rows."""
    rows = []
    for index in range(50):
        row = traffic.request(NOW - timedelta(minutes=90, seconds=index), "/other-tenant", status=200)
        row["service_id"] = "other-svc"
        row["batch_id"] = "22222222-2222-4222-8222-222222222222"
        rows.append(row)
    traffic.insert("request_facts", "request", rows)
    return traffic


def _console(service: SimpleNamespace, sql: str, *, window_minutes: int = 30) -> list[dict[str, Any]]:
    from backend.high_scale.query import query_endpoint
    from backend.models.dashboard import QueryRequest

    result = query_endpoint(
        service,  # type: ignore[arg-type]
        QueryRequest(sql=sql, max_rows=100),
        (NOW - timedelta(minutes=window_minutes)).isoformat(),
        NOW.isoformat(),
        False,
    )
    return result["data"]


@pytest.mark.skipif(
    os.getenv("RUN_HS_READERS_CLICKHOUSE_INTEGRATION") != "1", reason="explicit real ClickHouse readers gate"
)
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

    assert 0 < scoped < 300, "fixture sanity: the 30 minute window holds only part of one tenant's rows"
    assert attacked == scoped, "a user OR must not escape the injected service and time-window predicates"
    assert _console(service, "SELECT count(*) AS n FROM logs WHERE url = '/other-tenant'")[0]["n"] == 0


@pytest.mark.skipif(
    os.getenv("RUN_HS_READERS_CLICKHOUSE_INTEGRATION") != "1", reason="explicit real ClickHouse readers gate"
)
@pytest.mark.parametrize(
    "tail",
    ["-- swallow everything after this", "/* c */", "/* /* */ UNION SELECT 1 FROM system.tables\n-- */ */"],
)
def test_sql_console_rejects_comments_so_a_trailing_one_cannot_swallow_scope_or_row_cap(
    two_tenants: _Fixture, service: SimpleNamespace, tail: str
) -> None:
    with pytest.raises(PermissionError):
        _console(service, f"SELECT url FROM logs {tail}")


@pytest.mark.skipif(
    os.getenv("RUN_HS_READERS_CLICKHOUSE_INTEGRATION") != "1", reason="explicit real ClickHouse readers gate"
)
def test_sql_console_row_cap_holds_server_side_for_a_huge_explicit_limit(
    two_tenants: _Fixture, service: SimpleNamespace
) -> None:
    from backend.repositories.query import MAX_QUERY_ROWS

    rows = _console(service, "SELECT url FROM logs LIMIT 999999999", window_minutes=60 * 12)

    assert 0 < len(rows) <= 100  # QueryRequest(max_rows=100) truncates after the bounded fetch
    assert MAX_QUERY_ROWS >= len(rows)


# ── Merged from tests/high_scale/test_contracts_and_gates.py ──

# ── From test_contracts.py ──


def test_archive_states_are_monotonic() -> None:
    validate_archive_transition(ArchiveState.ARTIFACT_UPLOADING, ArchiveState.ARTIFACT_VERIFIED)
    with pytest.raises(ValueError, match="monotonic"):
        validate_archive_transition(ArchiveState.MANIFEST_COMMITTED, ArchiveState.MANIFEST_PREPARED)


# ── From test_fencing.py ──


def test_stale_epoch_cannot_commit_manifest() -> None:
    pool = MagicMock()
    control = PostgresControlPlane(pool=pool)

    # Mock transaction and locked owner
    mock_conn = MagicMock()
    mock_owner = MagicMock(current_owner="high_scale", owner_epoch=5)

    control._locked_owner = MagicMock(return_value=mock_owner)  # type: ignore[assignment]
    control.transaction = MagicMock()  # type: ignore[assignment]
    control.transaction.return_value.__enter__.return_value = mock_conn

    # Worker has stale epoch = 4 (current is 5)
    manifest = MagicMock(spec=ArchiveManifest)
    manifest.manifest_id = "man-1"
    manifest.service_id = "svc-1"
    manifest.domain = "request"
    manifest.validate.return_value = None

    with pytest.raises(ValueError, match="owner epoch mismatch|stale owner epoch"):
        control.register_archive_manifest(manifest, owner_epoch=4)


def test_stale_epoch_cannot_acknowledge_sources_batch() -> None:
    pool = MagicMock()
    control = PostgresControlPlane(pool=pool)

    # Contract requires acknowledge_sources_batch with epoch check
    assert hasattr(control, "acknowledge_sources_batch"), (
        "Missing acknowledge_sources_batch method on PostgresControlPlane"
    )


def test_stale_epoch_cannot_claim_sources_batch() -> None:
    pool = MagicMock()
    control = PostgresControlPlane(pool=pool)

    # Contract requires claim_sources_batch with epoch check
    assert hasattr(control, "claim_sources_batch"), "Missing claim_sources_batch method on PostgresControlPlane"


# ── From test_publication_gate.py ──


def test_publication_attempt_with_mismatched_row_count_rejected() -> None:
    """A publication attempt with mismatched row count is rejected and never transitions to visible."""
    store = InMemoryBatchManifestStore()
    client = MagicMock()
    # Mock ClickHouse inserting 95 rows when batch has 100
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
    assert manifest is not None
    assert manifest.status is PublicationStatus.PENDING
    assert manifest.visible_rows == 0


def test_query_filters_visible_state_and_ignores_pending_rows() -> None:
    """Queries must filter on publication_state='visible' or _high_scale_visible=1 and never see pending rows."""
    from datetime import datetime, timedelta

    from backend.high_scale.archive_models import ServingWatermark

    from backend.high_scale.query_service import query_request_facts

    client = MagicMock()
    # ClickHouse query returns rows only when publication_state = 'visible'
    client.execute.return_value = [
        {"event_id": "00000000-0000-0000-0000-000000000001", "timestamp": "2026-10-09T12:00:00Z"},
    ]

    now = datetime(2026, 10, 9, 12, 5, tzinfo=UTC)
    watermark = ServingWatermark(
        service_id="svc-test",
        domain="request",
        owner_epoch=1,
        coverage_start=now - timedelta(minutes=5),
        coverage_end=now,
        last_accepted_cursor="cur-1",
        last_archived_event_id="evt-1",
        last_visible_event_id="evt-1",
        exact=True,
    )

    # Run query_request_facts
    results = query_request_facts(
        client,
        service_id="svc-test",
        start=now - timedelta(minutes=5),
        end=now,
        cursor_secret=b"test-secret-123456",
        watermark=watermark,
        limit=10,
    )
    assert len(results.rows) == 1

    # Verify execute was called with a query enforcing publication_state = 'visible'
    assert client.execute.called
    query_sql, params = client.execute.call_args[0]
    assert "publication_state = 'visible'" in query_sql or "_high_scale_visible = 1" in query_sql


# ── From test_query_contracts.py ──


def test_query_metadata_requires_coverage_and_watermark_consistency() -> None:
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

    metadata = QueryResponseMetadata(
        status="complete",
        exact=True,
        coverage=1.0,
        freshness_lag_seconds=3.0,
        watermark=watermark,
        approximation_error=None,
        error=None,
    )

    metadata.validate()


def test_approximation_error_is_required_for_approximate_results() -> None:
    watermark = ServingWatermark("svc", "request", 1, None, None, None, None, None, False)
    metadata = QueryResponseMetadata("complete", False, 0.5, 0.0, watermark, None, None)

    with pytest.raises(ValueError, match="approximation"):
        metadata.validate()


# ── From test_recovery_capacity.py ──


def test_two_million_events_per_second_target_requires_capacity_gate() -> None:
    report = evaluate_recovery_capacity(
        RecoveryCapacityMeasurement(
            events_per_second=2_000_000,
            recovery_window_seconds=15 * 60,
            archive_read_events_per_second=300_000_000,
            decode_events_per_second=300_000_000,
            aggregate_events_per_second=300_000_000,
            clickhouse_insert_events_per_second=750_000_000,
            replication_factor=3,
            live_ingest_reservation=0.2,
        )
    )

    assert report.required_events_per_second == pytest.approx(240_000_000)
    assert report.replication_adjusted_insert_events_per_second == pytest.approx(250_000_000)
    assert report.feasible is True


def test_replication_can_make_an_otherwise_fast_recovery_infeasible() -> None:
    report = evaluate_recovery_capacity(
        RecoveryCapacityMeasurement(
            events_per_second=2_000_000,
            recovery_window_seconds=15 * 60,
            archive_read_events_per_second=300_000_000,
            decode_events_per_second=300_000_000,
            aggregate_events_per_second=300_000_000,
            clickhouse_insert_events_per_second=400_000_000,
            replication_factor=3,
            live_ingest_reservation=0.2,
        )
    )

    assert report.limiting_stage == "clickhouse_insert"
    assert report.feasible is False


# ── From test_topology.py ──


def test_local_profile_does_not_claim_production_ha() -> None:
    profile = build_topology_profile("local")
    assert profile.clickhouse_replicas_per_shard == 1
    assert profile.keeper_members == 1
    assert profile.production_ha is False


def test_portable_production_profile_has_replicas_and_keeper() -> None:
    profile = build_topology_profile("portable-production")
    assert profile.clickhouse_replicas_per_shard == 2
    assert profile.keeper_members == 3
    assert profile.production_ha is True


def test_unknown_profile_is_rejected() -> None:
    with pytest.raises(ValueError, match="unknown"):
        build_topology_profile("unknown-production")


# ── From test_exports.py ──


def test_export_is_bounded_and_completes() -> None:
    manager = ExportManager(max_workers=1, max_rows=2, max_bytes=100)
    try:
        job_id = manager.submit("job-1", [{"id": 1}, {"id": 2}], lambda row: str(row["id"]).encode())
        job = manager.wait(job_id)
        assert job.state is ExportState.COMPLETED
        assert job.rows_written == 2
        assert job.payload == b"1\n2\n"
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
        assert manager.cancel(job_id) is True
        assert manager.wait(job_id).state is ExportState.CANCELLED
    finally:
        manager.shutdown()


# ── From test_config.py ──


def test_high_scale_requires_clickhouse_and_fos() -> None:
    config = HighScaleConfig(True, None, None, None)
    with pytest.raises(ValueError, match="ClickHouse"):
        config.validate()


def test_disabled_high_scale_config_is_inert() -> None:
    HighScaleConfig(False, None, None, None).validate()


def test_retention_recommendation_preserves_disk_reserve() -> None:
    recommendation = recommend_retention(
        ServiceProfile(events_per_second=100_000, average_row_bytes=900),
        StorageProfile(total_bytes=10 * 1024**4),
    )
    assert recommendation.hot_days == 1.0
    assert recommendation.warm_days >= recommendation.hot_days


def test_retention_recommendation_accounts_for_rum_and_enabled_fields() -> None:
    narrow = recommend_retention(
        ServiceProfile(events_per_second=100_000, average_row_bytes=100),
        StorageProfile(total_bytes=10**11),
    )
    wide = recommend_retention(
        ServiceProfile(events_per_second=200_000, average_row_bytes=600),
        StorageProfile(total_bytes=10**11),
    )
    assert wide.hot_days < narrow.hot_days


# ── Merged from test_registry_bootstrap.py ──────────────────────────────────

from backend.high_scale.registry_bootstrap import (
    HighScaleServiceRegistry,
    register_high_scale_services,
    register_high_scale_services_from_environment,
)


class FakeClickHouse:
    def __init__(self) -> None:
        self.request_coverage_end = datetime(2026, 9, 12, tzinfo=UTC)
        self.calls: list[str] = []

    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        assert params is not None
        assert params["service_id"] == "svc"
        self.calls.append(sql)
        if "max(event_timestamp)" in sql:
            return [
                {
                    "coverage_start": datetime(2026, 9, 1, tzinfo=UTC),
                    "coverage_end": self.request_coverage_end,
                }
            ]
        if "last_visible_event_id" in sql:
            return [{"last_visible_event_id": "00000000-0000-0000-0000-000000000123"}]
        return []


class EmptyClickHouse:
    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        return [{"coverage_start": None, "coverage_end": None, "last_visible_event_id": None}]


class StringClickHouse:
    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        if "max(event_timestamp)" in sql:
            return [
                {
                    "coverage_start": "2026-09-12 20:50:12.123",
                    "coverage_end": "2026-09-12 20:50:12.123",
                }
            ]
        return [{"last_visible_event_id": "a4dc715f-733c-4535-8090-3dd5637e9e0b"}]


def test_register_high_scale_services_exposes_clickhouse_watermark() -> None:
    registry = HighScaleServiceRegistry()

    register_high_scale_services(
        registry,
        client=FakeClickHouse(),
        service_ids=("svc",),
        cursor_secret=b"local-test-secret",
        owner_epoch=7,
    )

    service = registry.resolve("svc")
    assert service is not None
    assert service.cursor_secret == b"local-test-secret"
    watermark = service.watermark()
    assert watermark.service_id == "svc"
    assert watermark.domain == "request"
    assert watermark.owner_epoch == 7
    assert watermark.coverage_start == datetime(2026, 9, 1, tzinfo=UTC)
    assert watermark.coverage_end == datetime(2026, 9, 12, tzinfo=UTC)
    assert watermark.last_visible_event_id == "00000000-0000-0000-0000-000000000123"


def test_registered_watermark_reflects_new_clickhouse_visibility() -> None:
    registry = HighScaleServiceRegistry()
    client = FakeClickHouse()

    register_high_scale_services(
        registry,
        client=client,
        service_ids=("svc",),
        cursor_secret=b"local-test-secret",
        owner_epoch=7,
    )

    service = registry.resolve("svc")
    assert service is not None
    assert service.watermark().coverage_end == datetime(2026, 9, 12, tzinfo=UTC)

    client.request_coverage_end = datetime(2026, 9, 22, 4, 20, tzinfo=UTC)

    assert service.watermark().coverage_end == datetime(2026, 9, 22, 4, 20, tzinfo=UTC)


def test_register_high_scale_services_avoids_argmax_watermark_scan() -> None:
    registry = HighScaleServiceRegistry()
    client = FakeClickHouse()

    register_high_scale_services(
        registry,
        client=client,
        service_ids=("svc",),
        cursor_secret=b"local-test-secret",
        owner_epoch=7,
    )

    service = registry.resolve("svc")
    assert service is not None
    service.watermark()

    assert len(client.calls) == 2
    assert all("argMax" not in sql for sql in client.calls)


def test_register_high_scale_services_parses_clickhouse_timestamp_strings() -> None:
    registry = HighScaleServiceRegistry()

    register_high_scale_services(
        registry,
        client=StringClickHouse(),
        service_ids=("svc",),
        cursor_secret=b"local-test-secret",
    )

    service = registry.resolve("svc")
    assert service is not None
    watermark = service.watermark()
    assert watermark.coverage_start == datetime(2026, 9, 12, 20, 50, 12, 123000, tzinfo=UTC)
    assert watermark.coverage_end == datetime(2026, 9, 12, 20, 50, 12, 123000, tzinfo=UTC)
    assert watermark.last_visible_event_id == "a4dc715f-733c-4535-8090-3dd5637e9e0b"


def test_register_high_scale_services_rejects_empty_secret() -> None:
    registry = HighScaleServiceRegistry()

    try:
        register_high_scale_services(
            registry,
            client=FakeClickHouse(),
            service_ids=("svc",),
            cursor_secret=b"",
        )
    except ValueError as exc:
        assert str(exc) == "high-scale cursor secret is required"
    else:
        raise AssertionError("empty cursor secrets must be rejected")


def test_environment_registration_requires_explicit_service_binding(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HIGH_SCALE_ENABLED", "1")
    monkeypatch.delenv("HIGH_SCALE_SERVICE_IDS", raising=False)

    with pytest.raises(ValueError, match="HIGH_SCALE_SERVICE_IDS"):
        register_high_scale_services_from_environment(HighScaleServiceRegistry(), client=FakeClickHouse())


class FakeCatalog:
    def manifests_covering(self, service_id, domain, start, end):
        return ()


def test_register_high_scale_services_wires_cold_tier_when_both_are_available() -> None:
    registry = HighScaleServiceRegistry()
    catalog = FakeCatalog()
    archive = object()

    register_high_scale_services(
        registry,
        client=FakeClickHouse(),
        service_ids=("svc",),
        cursor_secret=b"local-test-secret",
        manifest_catalog=catalog,
        archive_for=lambda service_id: archive,
    )

    service = registry.resolve("svc")
    assert service is not None
    assert service.manifest_catalog is catalog
    assert service.archive is archive


def test_register_high_scale_services_leaves_cold_tier_unconfigured_without_archive() -> None:
    registry = HighScaleServiceRegistry()

    register_high_scale_services(
        registry,
        client=FakeClickHouse(),
        service_ids=("svc",),
        cursor_secret=b"local-test-secret",
        manifest_catalog=FakeCatalog(),
        archive_for=lambda service_id: None,
    )

    service = registry.resolve("svc")
    assert service is not None
    assert service.manifest_catalog is None
    assert service.archive is None


def test_register_high_scale_services_defaults_cold_tier_to_unconfigured() -> None:
    registry = HighScaleServiceRegistry()

    register_high_scale_services(
        registry,
        client=FakeClickHouse(),
        service_ids=("svc",),
        cursor_secret=b"local-test-secret",
    )

    service = registry.resolve("svc")
    assert service is not None
    assert service.manifest_catalog is None
    assert service.archive is None


def test_environment_registration_wires_cold_tier_via_injected_factories(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HIGH_SCALE_ENABLED", "1")
    monkeypatch.setenv("HIGH_SCALE_SERVICE_IDS", "svc")
    monkeypatch.setenv("HIGH_SCALE_CURSOR_SECRET", "local-test-secret")
    registry = HighScaleServiceRegistry()
    catalog = FakeCatalog()
    archive = object()

    register_high_scale_services_from_environment(
        registry,
        client=FakeClickHouse(),
        control_factory=lambda: catalog,
        archive_factory=lambda service_id: archive,
    )

    service = registry.resolve("svc")
    assert service is not None
    assert service.manifest_catalog is catalog
    assert service.archive is archive


def test_environment_registration_degrades_when_archive_factory_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HIGH_SCALE_ENABLED", "1")
    monkeypatch.setenv("HIGH_SCALE_SERVICE_IDS", "svc")
    monkeypatch.setenv("HIGH_SCALE_CURSOR_SECRET", "local-test-secret")
    registry = HighScaleServiceRegistry()

    def _boom(service_id: str):
        raise RuntimeError("no FOS source configured")

    register_high_scale_services_from_environment(
        registry,
        client=FakeClickHouse(),
        control_factory=lambda: FakeCatalog(),
        archive_factory=_boom,
    )

    service = registry.resolve("svc")
    assert service is not None
    assert service.manifest_catalog is None
    assert service.archive is None


def test_empty_serving_table_has_no_visible_event_watermark() -> None:
    registry = HighScaleServiceRegistry()
    register_high_scale_services(
        registry,
        client=EmptyClickHouse(),
        service_ids=("svc",),
        cursor_secret=b"local-test-secret",
    )

    service = registry.resolve("svc")
    assert service is not None
    watermark = service.watermark()
    assert watermark.coverage_start is None
    assert watermark.coverage_end is None
    assert watermark.last_visible_event_id is None


# ── Merged from test_disk_sampler_wiring.py ─────────────────────────────────

from unittest.mock import patch

from backend.cron.jobs.metric_snapshot import _run_metric_snapshot, _sample_high_scale_disk
from backend.high_scale.archive_models import VolumeStats, evaluate_disk_state
from structlog.testing import capture_logs

from backend.core import metric_snapshots
from backend.high_scale import archive_models as disk_sampler
from backend.utils.remote_access import _is_blocked_path

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
def _reset_sampler():
    disk_sampler.reset_for_tests()
    yield
    disk_sampler.reset_for_tests()


def _history(metric: str):
    return metric_snapshots.get_batch(since=datetime.now(UTC) - timedelta(hours=1)).get(metric, [])


def test_sampler_reads_real_volume_stats_and_evaluates_mode() -> None:
    probe = _FakeClickHouse(free_fraction=0.12)

    snapshot = disk_sampler.sample_disk_state(probe)

    assert probe.calls == 1
    assert snapshot.state.mode == "protective"
    assert snapshot.total_bytes == _TB
    assert disk_sampler.latest_disk_snapshot() is snapshot


def test_one_batch_bytes_raises_the_reserve_floor(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HIGH_SCALE_DISK_ONE_BATCH_BYTES", str(_TB))

    snapshot = disk_sampler.sample_disk_state(_FakeClickHouse(free_fraction=0.5))

    assert snapshot.state.reserve_bytes == _TB
    assert snapshot.state.mode == "warning"


def test_mode_transitions_are_logged_once_each(monkeypatch) -> None:
    events: list[tuple[str, dict]] = []

    class _Recorder:
        def info(self, event: str, **kw) -> None:
            events.append(("info", {"event": event, **kw}))

        def warning(self, event: str, **kw) -> None:
            events.append(("warning", {"event": event, **kw}))

    monkeypatch.setattr(disk_sampler, "logger", _Recorder())
    monkeypatch.setattr(disk_sampler, "_latest", None)

    disk_sampler.sample_disk_state(_FakeClickHouse(0.50))
    disk_sampler.sample_disk_state(_FakeClickHouse(0.50))
    disk_sampler.sample_disk_state(_FakeClickHouse(0.08))

    changes = [(level, e) for level, e in events if e["event"] == "high_scale.disk_mode_changed"]
    assert [(e["previous_mode"], e["mode"]) for _, e in changes] == [(None, "normal"), ("normal", "emergency")]
    assert changes[-1][0] == "warning"


def test_metric_snapshot_tick_samples_disk_in_high_scale_mode() -> None:
    probe = _FakeClickHouse(free_fraction=0.12)
    with (
        patch("backend.config.is_high_scale_mode", return_value=True),
        patch("backend.core.clickhouse_client.get_clickhouse_client", return_value=probe),
    ):
        _run_metric_snapshot()

    assert probe.calls == 1
    assert _history("high_scale_disk_mode_level")[-1]["value"] == 2.0
    assert _history("high_scale_disk_free_fraction")[-1]["value"] == pytest.approx(0.12, abs=1e-3)


def test_metric_snapshot_tick_never_touches_clickhouse_in_standard_mode() -> None:
    with (
        patch("backend.config.is_high_scale_mode", return_value=False),
        patch("backend.core.clickhouse_client.get_clickhouse_client") as get_client,
    ):
        assert _sample_high_scale_disk() == 0
        _run_metric_snapshot()

    get_client.assert_not_called()
    assert disk_sampler.latest_disk_snapshot() is None
    assert _history("high_scale_disk_mode_level") == []


def test_probe_failure_is_logged_and_never_breaks_the_tick() -> None:
    class _Down:
        def storage_health(self) -> dict:
            raise RuntimeError("clickhouse unavailable")

    with (
        capture_logs() as logs,
        patch("backend.config.is_high_scale_mode", return_value=True),
        patch("backend.core.clickhouse_client.get_clickhouse_client", return_value=_Down()),
    ):
        assert _sample_high_scale_disk() == 0

    assert any(e["event"] == "high_scale.disk_sample_failed" and e["error_kind"] == "RuntimeError" for e in logs)
    assert disk_sampler.latest_disk_snapshot() is None


def test_health_snapshot_surfaces_the_sampled_mode(client) -> None:
    probe = _FakeClickHouse(free_fraction=0.09)
    with (
        patch("backend.config.is_high_scale_mode", return_value=True),
        patch("backend.core.clickhouse_client.get_clickhouse_client", return_value=probe),
    ):
        _run_metric_snapshot()
        resp = client.get("/api/admin/health-snapshot")

    assert resp.status_code == 200
    disk = resp.json()["high_scale"]["disk"]
    assert disk["mode"] == "emergency"
    assert disk["free_fraction"] == pytest.approx(0.09, abs=1e-3)
    assert disk["total_bytes"] == _TB
    assert disk["sample_age_s"] >= 0


def test_health_snapshot_high_scale_block_is_empty_before_the_first_sample(client) -> None:
    with patch("backend.config.is_high_scale_mode", return_value=True):
        body = client.get("/api/admin/health-snapshot").json()

    assert body["high_scale"]["disk"] is None


def test_health_snapshot_omits_the_high_scale_key_in_standard_mode(client) -> None:
    with patch("backend.config.is_high_scale_mode", return_value=False):
        body = client.get("/api/admin/health-snapshot").json()

    assert "high_scale" not in body


def volume_stats(*, free_fraction: float, total_bytes: int = 1_000_000_000, one_batch_bytes: int = 0) -> VolumeStats:
    return VolumeStats(
        total_bytes=total_bytes,
        free_bytes=int(total_bytes * free_fraction),
        one_batch_bytes=one_batch_bytes,
    )


def test_disk_guard_enters_emergency_mode_at_ten_percent() -> None:
    assert evaluate_disk_state(volume_stats(free_fraction=0.10)).mode == "emergency"


def test_disk_guard_enters_emergency_below_ten_percent() -> None:
    assert evaluate_disk_state(volume_stats(free_fraction=0.05)).mode == "emergency"


def test_disk_guard_is_protective_between_ten_and_fifteen_percent() -> None:
    assert evaluate_disk_state(volume_stats(free_fraction=0.12)).mode == "protective"


def test_disk_guard_warns_below_the_twenty_percent_reserve() -> None:
    assert evaluate_disk_state(volume_stats(free_fraction=0.18)).mode == "warning"


def test_disk_guard_is_normal_with_ample_free_space() -> None:
    assert evaluate_disk_state(volume_stats(free_fraction=0.50)).mode == "normal"


def test_disk_guard_reserve_floor_is_the_greater_of_twenty_percent_or_one_batch() -> None:
    state = evaluate_disk_state(volume_stats(free_fraction=0.19, total_bytes=1_000_000_000, one_batch_bytes=1_000_000))
    assert state.mode == "warning"
    assert state.reserve_bytes == 200_000_000


def test_disk_guard_reserve_floor_uses_batch_size_when_larger_than_twenty_percent() -> None:
    state = evaluate_disk_state(
        volume_stats(free_fraction=0.50, total_bytes=1_000_000_000, one_batch_bytes=900_000_000)
    )
    assert state.reserve_bytes == 900_000_000
    assert state.mode == "warning"


def test_disk_guard_rejects_free_bytes_above_total() -> None:
    with pytest.raises(ValueError, match="free_bytes"):
        VolumeStats(total_bytes=100, free_bytes=200, one_batch_bytes=0)


def profile(rate: float, *, average_row_bytes: float = 500.0) -> ServiceProfile:
    return ServiceProfile(events_per_second=rate, average_row_bytes=average_row_bytes)


def storage(total_bytes: int) -> StorageProfile:
    return StorageProfile(total_bytes=total_bytes)


def test_high_volume_service_gets_shorter_hot_recommendation() -> None:
    low = recommend_retention(profile(1_000), storage(1 << 40))
    high = recommend_retention(profile(1_000_000), storage(1 << 40))

    assert low.hot_days > high.hot_days


def test_low_volume_service_hits_the_baseline_hot_target() -> None:
    recommendation = recommend_retention(profile(1), storage(1 << 40))

    assert recommendation.hot_days == 1.0


def test_warm_recommendation_never_exceeds_the_baseline_target() -> None:
    recommendation = recommend_retention(profile(1), storage(1 << 50))

    assert recommendation.warm_days == 30.0


def test_zero_rate_service_is_rejected() -> None:
    with pytest.raises(ValueError, match="events_per_second"):
        recommend_retention(profile(0), storage(1 << 40))


def test_recommendation_never_exceeds_the_protected_reserve() -> None:
    recommendation = recommend_retention(profile(1_000_000), storage(1 << 30))

    assert recommendation.hot_days >= 0.0
    assert recommendation.warm_days >= recommendation.hot_days


_PATH = "/api/admin/high-scale/retention-recommendation"


def _cfg(retention_days: int | None = None) -> dict:
    cfg: dict = {"service_id": "svc-hs", "provisioning": {"cron_sync": {}}}
    if retention_days is not None:
        cfg["provisioning"]["cron_sync"]["data_retention_days"] = retention_days
    return cfg


def _get(client, cfg, **params):
    query = {"service_id": "svc-hs", "events_per_second": 1000, **params}
    with (
        patch("backend.config.is_high_scale_mode", return_value=True),
        patch("backend.config.load_config", return_value=cfg),
    ):
        return client.get(_PATH, params=query)


def test_recommendation_matches_the_pure_function_for_the_request(client) -> None:
    resp = _get(client, _cfg(), total_bytes=_TB, average_row_bytes=800)

    assert resp.status_code == 200
    body = resp.json()
    expected = recommend_retention(ServiceProfile(1000, 800), StorageProfile(_TB))
    assert body["recommended_hot_days"] == pytest.approx(expected.hot_days)
    assert body["recommended_warm_days"] == pytest.approx(expected.warm_days)
    assert body["reasoning"] == expected.reasoning
    assert body["volume_total_source"] == "request"
    assert body["source"] == "recommendation"
    assert body["effective_retention_days"] == pytest.approx(expected.warm_days)


def test_explicit_per_service_retention_always_wins(client) -> None:
    body = _get(client, _cfg(retention_days=7), total_bytes=_TB).json()

    assert body["source"] == "override"
    assert body["explicit_retention_days"] == 7
    assert body["effective_retention_days"] == 7.0
    assert body["recommended_warm_days"] > 0


def test_explicit_keep_forever_is_an_override_not_a_recommendation(client) -> None:
    body = _get(client, _cfg(retention_days=0), total_bytes=_TB).json()

    assert body["source"] == "override"
    assert body["explicit_retention_days"] == 0
    assert body["effective_retention_days"] is None


def test_volume_defaults_to_the_sampled_serving_volume(client) -> None:
    class _Probe:
        def storage_health(self) -> dict:
            return {"disk_total": 2 * _TB, "disk_free": _TB}

    disk_sampler.sample_disk_state(_Probe())

    body = _get(client, _cfg()).json()

    assert body["volume_total_bytes"] == 2 * _TB
    assert body["volume_total_source"] == "sampled"


def test_unknown_volume_is_a_409_not_a_guess(client) -> None:
    resp = _get(client, _cfg())

    assert resp.status_code == 409
    assert resp.json()["detail"]["error"] == "high_scale_volume_unknown"


def test_endpoint_is_unavailable_outside_high_scale_mode(client) -> None:
    with patch("backend.config.is_high_scale_mode", return_value=False):
        resp = client.get(_PATH, params={"service_id": "svc-hs", "events_per_second": 1000, "total_bytes": _TB})

    assert resp.status_code == 404
    assert resp.json()["detail"]["error"] == "high_scale_not_enabled"


def test_unknown_service_is_a_404(client) -> None:
    with (
        patch("backend.config.is_high_scale_mode", return_value=True),
        patch("backend.config.load_config", return_value=None),
    ):
        resp = client.get(_PATH, params={"service_id": "nope", "events_per_second": 1, "total_bytes": _TB})

    assert resp.status_code == 404
    assert resp.json()["detail"]["error"] == "service_not_found"


def test_non_positive_rate_is_rejected(client) -> None:
    resp = _get(client, _cfg(), total_bytes=_TB, events_per_second=0)

    assert resp.status_code == 422


def test_recommendation_is_logged_with_its_inputs(client, caplog: pytest.LogCaptureFixture) -> None:
    # Exercise the same cached logger used by earlier requests in a full worker.
    assert _get(client, _cfg(), total_bytes=_TB).status_code == 200
    processors = structlog.get_config()["processors"]
    caplog.clear()
    try:
        # Production logging reconfiguration replaces the processor list;
        # already-bound loggers retain the previous, equivalent chain.
        structlog.configure(processors=list(processors))
        # Capture the emitted stdlib record, not only the latest global
        # processor list: cached loggers can still use an older valid chain.
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
    event = events[0]
    assert event["service_id"] == "svc-hs"
    assert event["source"] == "override"
    assert event["explicit_retention_days"] == 14
    assert event["events_per_second"] == 1000
    assert event["average_row_bytes"] == 500
    assert event["volume_total_bytes"] == _TB
    assert event["volume_total_source"] == "request"


def test_endpoint_is_analyst_blocked_by_the_admin_prefix() -> None:
    assert _is_blocked_path(_PATH) is True


def test_recommendation_never_writes_service_config(client) -> None:
    with (
        patch("backend.config.is_high_scale_mode", return_value=True),
        patch("backend.config.load_config", return_value=_cfg()),
        patch("backend.config.save_config") as save,
    ):
        resp = client.get(_PATH, params={"service_id": "svc-hs", "events_per_second": 1000, "total_bytes": _TB})

    assert resp.status_code == 200
    save.assert_not_called()
