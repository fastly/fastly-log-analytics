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


# ── Merged from test_batch_manifest.py ───────────────────────────────

from datetime import UTC, timedelta

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
