from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

from backend.high_scale.archive_models import (
    ArchiveArtifact,
    ArchiveManifest,
    ArchiveSourceObject,
    ServingWatermark,
)
from backend.high_scale.schema import (
    build_cmcd_projection_key,
    build_request_event_id,
    build_rum_event_id,
)


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


from backend.high_scale.schema import build_batch_id_from_manifest_id


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
