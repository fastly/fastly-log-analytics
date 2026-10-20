import hashlib
import json
from datetime import UTC, datetime
from io import BytesIO

import pyarrow as pa
import pyarrow.parquet as pq

from backend.high_scale.archive_models import ArchiveArtifact, ArchiveManifest, ArchiveSourceObject
from backend.high_scale.archive_publication import ArchivePublication, InMemoryObjectStore
from backend.high_scale.publication import ClickHousePublication, InMemoryBatchManifestStore, InsertReceipt
from backend.high_scale.replay import replay_manifest


class ReplayClient:
    def insert(self, batch):
        return InsertReceipt(batch.batch_id, len(batch.rows), batch.digest)


def test_replay_rebuilds_visibility_from_archive_only() -> None:
    rows = ({"event_id": "event-1", "status": 200}, {"event_id": "event-2", "status": 500})
    artifact_bytes = _parquet(rows)
    source = ArchiveSourceObject("svc", "request", "raw/request/a.gz", "sha256:source", 4, "v1")
    manifest = ArchiveManifest(
        "manifest-1",
        source,
        ArchiveArtifact(
            "s3://archive/artifacts/manifest-1.parquet",
            _checksum(artifact_bytes),
            len(artifact_bytes),
            len(rows),
            2,
            _event_digest(rows),
            "request.v1",
            "normalize.v1",
        ),
        datetime(2026, 9, 1, tzinfo=UTC),
        datetime(2026, 9, 1, 0, 1, tzinfo=UTC),
        datetime(2026, 9, 2, tzinfo=UTC),
        datetime(2026, 9, 3, tzinfo=UTC),
        4,
    )
    store = InMemoryObjectStore()
    archive = ArchivePublication(store)
    archive.publish(manifest, artifact_bytes)
    publication = ClickHousePublication(InMemoryBatchManifestStore(), ReplayClient())

    result = replay_manifest(manifest, archive, publication)

    assert result.rows_read == 2
    assert result.publication.rows_visible == 2


def test_replay_with_a_custom_batch_id_reinserts_after_a_prior_replay_is_marked_visible() -> None:
    """A recovery rebuild must not silently skip a manifest just because an
    EARLIER replay of it (under the default batch id) is still recorded as
    visible in the manifest store — that bookkeeping can outlive the actual
    ClickHouse data (e.g. after a real ClickHouse volume loss, or in this
    test, an explicit wipe), so a rebuild needs a fresh batch id scope."""
    rows = ({"event_id": "event-1", "status": 200},)
    artifact_bytes = _parquet(rows)
    source = ArchiveSourceObject("svc", "request", "raw/request/a.gz", "sha256:source", 4, "v1")
    manifest = ArchiveManifest(
        "manifest-1",
        source,
        ArchiveArtifact(
            "s3://archive/artifacts/manifest-1.parquet",
            _checksum(artifact_bytes),
            len(artifact_bytes),
            len(rows),
            1,
            _event_digest(rows),
            "request.v1",
            "normalize.v1",
        ),
        datetime(2026, 9, 1, tzinfo=UTC),
        datetime(2026, 9, 1, 0, 1, tzinfo=UTC),
        datetime(2026, 9, 2, tzinfo=UTC),
        datetime(2026, 9, 3, tzinfo=UTC),
        4,
    )
    store = InMemoryObjectStore()
    archive = ArchivePublication(store)
    archive.publish(manifest, artifact_bytes)

    inserted: list[int] = []

    class CountingClient:
        def insert(self, batch):
            inserted.append(len(batch.rows))
            return InsertReceipt(batch.batch_id, len(batch.rows), batch.digest)

    manifests = InMemoryBatchManifestStore()
    publication = ClickHousePublication(manifests, CountingClient())

    first = replay_manifest(manifest, archive, publication)
    assert first.publication.duplicate is False
    assert inserted == [1]

    # Default batch id: already visible per the manifest store, so a second
    # replay under the SAME id must skip re-inserting (the existing,
    # intentional idempotency contract for a genuine retry).
    second = replay_manifest(manifest, archive, publication)
    assert second.publication.duplicate is True
    assert inserted == [1]

    # A rebuild-scoped batch id has never been seen before, so it correctly
    # re-inserts even though the manifest itself was already replayed.
    third = replay_manifest(manifest, archive, publication, batch_id=f"rebuild:xyz:{manifest.manifest_id}")
    assert third.publication.duplicate is False
    assert inserted == [1, 1]


def test_replay_manifest_filters_mixed_rum_rows_by_serving_domain() -> None:
    """Raw RUM objects share raw/rum/ across vitals and errors, so a rum_errors
    archive manifest may contain only vitals rows (empty error_message) or a mix.
    replay_manifest must filter with row_matches_serving_domain and skip publishing
    empty batches."""
    vitals_only_rows = (
        {
            "event_id": "vital-1",
            "metric_name": "LCP",
            "metric_value": 1200.0,
            "error_message": "",
        },
    )
    artifact_bytes = _parquet(vitals_only_rows)
    source = ArchiveSourceObject("svc", "rum_errors", "raw/rum/a.gz", "sha256:source", 4, "v1")
    manifest = ArchiveManifest(
        "manifest-rum-err-empty",
        source,
        ArchiveArtifact(
            "s3://archive/artifacts/manifest-rum-err-empty.parquet",
            _checksum(artifact_bytes),
            len(artifact_bytes),
            len(vitals_only_rows),
            1,
            _event_digest(vitals_only_rows),
            "rum_errors.v1",
            "normalize.v1",
        ),
        datetime(2026, 9, 1, tzinfo=UTC),
        datetime(2026, 9, 1, 0, 1, tzinfo=UTC),
        datetime(2026, 9, 2, tzinfo=UTC),
        datetime(2026, 9, 3, tzinfo=UTC),
        4,
    )
    store = InMemoryObjectStore()
    archive = ArchivePublication(store)
    archive.publish(manifest, artifact_bytes)

    inserted: list[int] = []

    class StrictRumErrorClient:
        def insert(self, batch):
            for row in batch.rows:
                if not row.get("error_message"):
                    raise ValueError("rum_errors batch contained non-error row")
            inserted.append(len(batch.rows))
            return InsertReceipt(batch.batch_id, len(batch.rows), batch.digest)

    publication = ClickHousePublication(InMemoryBatchManifestStore(), StrictRumErrorClient())
    result = replay_manifest(manifest, archive, publication)

    assert result.rows_read == 0
    assert result.publication.rows_visible == 0
    assert inserted == []


def _parquet(rows: tuple[dict[str, object], ...]) -> bytes:
    output = BytesIO()
    pq.write_table(pa.Table.from_pylist(list(rows)), output)
    return output.getvalue()


def _checksum(value: bytes) -> str:
    return f"sha256:{hashlib.sha256(value).hexdigest()}"


def _event_digest(rows: tuple[dict[str, object], ...]) -> str:
    digest = hashlib.sha256()
    for row in rows:
        digest.update((json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n").encode())
    return f"sha256:{digest.hexdigest()}"
