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
    archive_sql = (root / "archive_manifest_schema.sql").read_text()
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
    assert "manifest_committed" in archive_sql
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
