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
    assert "ReplicatedMergeTree" in request_sql
    assert "publication_state" in request_sql
    assert "rum_vitals_facts" in rum_sql and "rum_error_facts" in rum_sql
    assert "request_event_id" in cmcd_sql
    assert "manifest_committed" in archive_sql
