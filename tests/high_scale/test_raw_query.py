import hashlib
import json
from datetime import UTC, datetime
from io import BytesIO

import pyarrow as pa
import pyarrow.parquet as pq

from backend.high_scale.archive_models import ArchiveArtifact, ArchiveManifest, ArchiveSourceObject
from backend.high_scale.archive_publication import ArchivePublication, InMemoryObjectStore
from backend.high_scale.raw_query import raw_query_producer


class _Catalog:
    def __init__(self, manifests: tuple[ArchiveManifest, ...]) -> None:
        self._manifests = manifests

    def manifests_covering(self, service_id, domain, start, end):
        return tuple(
            m
            for m in self._manifests
            if m.source.service_id == service_id
            and m.source.domain == domain
            and m.coverage_end > start
            and m.coverage_start < end
        )


def _parquet(rows: tuple[dict, ...]) -> bytes:
    output = BytesIO()
    pq.write_table(pa.Table.from_pylist(list(rows)), output)
    return output.getvalue()


def _checksum(value: bytes) -> str:
    return f"sha256:{hashlib.sha256(value).hexdigest()}"


def _event_digest(rows: tuple[dict, ...]) -> str:
    digest = hashlib.sha256()
    for row in rows:
        digest.update((json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n").encode())
    return f"sha256:{digest.hexdigest()}"


def _manifest(manifest_id: str, coverage_start: datetime, coverage_end: datetime, rows: tuple[dict, ...]) -> tuple:
    artifact_bytes = _parquet(rows)
    source = ArchiveSourceObject("svc", "request", f"raw/request/{manifest_id}.gz", "sha256:source", 4, "v1")
    manifest = ArchiveManifest(
        manifest_id,
        source,
        ArchiveArtifact(
            f"s3://archive/artifacts/{manifest_id}.parquet",
            _checksum(artifact_bytes),
            len(artifact_bytes),
            len(rows),
            len(artifact_bytes),
            _event_digest(rows),
            "request.v1",
            "normalize.v1",
        ),
        coverage_start,
        coverage_end,
        datetime(2026, 9, 4, tzinfo=UTC),
        datetime(2026, 9, 5, tzinfo=UTC),
        1,
    )
    return manifest, artifact_bytes


def test_producer_yields_rows_from_all_manifests_covering_the_window() -> None:
    m1, bytes1 = _manifest(
        "m1",
        datetime(2026, 9, 1, 0, 0, tzinfo=UTC),
        datetime(2026, 9, 1, 0, 1, tzinfo=UTC),
        ({"event_id": "1", "timestamp": "2026-09-01T00:00:30Z"},),
    )
    m2, bytes2 = _manifest(
        "m2",
        datetime(2026, 9, 1, 0, 1, tzinfo=UTC),
        datetime(2026, 9, 1, 0, 2, tzinfo=UTC),
        ({"event_id": "2", "timestamp": "2026-09-01T00:01:30Z"},),
    )
    store = InMemoryObjectStore()
    archive = ArchivePublication(store)
    archive.publish(m1, bytes1)
    archive.publish(m2, bytes2)
    catalog = _Catalog((m1, m2))

    producer = raw_query_producer(
        catalog,
        archive,
        service_id="svc",
        domain="request",
        start=datetime(2026, 9, 1, 0, 0, tzinfo=UTC),
        end=datetime(2026, 9, 1, 0, 2, tzinfo=UTC),
    )

    rows = list(producer())

    assert [row["event_id"] for row in rows] == ["1", "2"]


def test_producer_excludes_rows_outside_the_requested_window() -> None:
    manifest, artifact_bytes = _manifest(
        "m1",
        datetime(2026, 9, 1, 0, 0, tzinfo=UTC),
        datetime(2026, 9, 1, 0, 2, tzinfo=UTC),
        (
            {"event_id": "early", "timestamp": "2026-09-01T00:00:10Z"},
            {"event_id": "in-window", "timestamp": "2026-09-01T00:01:00Z"},
            {"event_id": "late", "timestamp": "2026-09-01T00:01:50Z"},
        ),
    )
    store = InMemoryObjectStore()
    archive = ArchivePublication(store)
    archive.publish(manifest, artifact_bytes)
    catalog = _Catalog((manifest,))

    producer = raw_query_producer(
        catalog,
        archive,
        service_id="svc",
        domain="request",
        start=datetime(2026, 9, 1, 0, 0, 30, tzinfo=UTC),
        end=datetime(2026, 9, 1, 0, 1, 30, tzinfo=UTC),
    )

    rows = list(producer())

    assert [row["event_id"] for row in rows] == ["in-window"]


def test_producer_yields_nothing_when_no_manifest_covers_the_window() -> None:
    catalog = _Catalog(())
    store = InMemoryObjectStore()
    archive = ArchivePublication(store)

    producer = raw_query_producer(
        catalog,
        archive,
        service_id="svc",
        domain="request",
        start=datetime(2026, 9, 1, tzinfo=UTC),
        end=datetime(2026, 9, 1, 1, tzinfo=UTC),
    )

    assert list(producer()) == []
