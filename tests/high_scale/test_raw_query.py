from datetime import UTC, datetime

from backend.high_scale.archive_publication import ArchivePublication, InMemoryObjectStore
from backend.high_scale.raw_query import raw_query_producer
from tests.high_scale.conftest import (
    InMemoryManifestCatalog as _Catalog,
)
from tests.high_scale.conftest import (
    make_test_archive_manifest as _manifest,
)


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
