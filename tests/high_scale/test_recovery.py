import hashlib
import json
from datetime import UTC, datetime, timedelta
from io import BytesIO

import pyarrow as pa
import pyarrow.parquet as pq

from backend.high_scale.archive_models import ArchiveArtifact, ArchiveManifest, ArchiveSourceObject
from backend.high_scale.archive_publication import ArchivePublication, InMemoryObjectStore
from backend.high_scale.publication import ClickHousePublication, InMemoryBatchManifestStore, InsertReceipt
from backend.high_scale.recovery import rebuild_origin_projections_from_fos, rebuild_serving_state_from_fos

NOW = datetime(2026, 9, 14, 12, 0, tzinfo=UTC)


class ReplayClient:
    def __init__(self, *, fail_on: set[str] | None = None) -> None:
        self._fail_on = fail_on or set()
        self.insert_calls: list[str] = []

    def insert(self, batch):
        if any(needle in batch.batch_id for needle in self._fail_on):
            raise RuntimeError(f"simulated insert failure for {batch.batch_id}")
        self.insert_calls.append(batch.batch_id)
        return InsertReceipt(batch.batch_id, len(batch.rows), batch.digest)


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


def _manifest(manifest_id: str, coverage_start: datetime, coverage_end: datetime, rows: tuple[dict, ...]):
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
        NOW + timedelta(days=1),
        NOW + timedelta(days=2),
        1,
    )
    return manifest, artifact_bytes


class _Catalog:
    def __init__(self, manifests: tuple[ArchiveManifest, ...]) -> None:
        self._manifests = manifests

    def manifests_covering(self, service_id, domain, start, end):
        return tuple(m for m in self._manifests if m.coverage_end > start and m.coverage_start < end)


def test_recovery_prioritizes_recent_history_and_reports_full_completion() -> None:
    recent, recent_bytes = _manifest(
        "recent", NOW - timedelta(hours=1), NOW - timedelta(minutes=59), ({"event_id": "1"},)
    )
    warm, warm_bytes = _manifest(
        "warm", NOW - timedelta(days=10), NOW - timedelta(days=10) + timedelta(minutes=1), ({"event_id": "2"},)
    )
    store = InMemoryObjectStore()
    archive = ArchivePublication(store)
    archive.publish(recent, recent_bytes)
    archive.publish(warm, warm_bytes)
    catalog = _Catalog((recent, warm))
    publication = ClickHousePublication(InMemoryBatchManifestStore(), ReplayClient())

    report = rebuild_serving_state_from_fos(catalog, archive, publication, service_id="svc", domain="request", now=NOW)

    assert report.manifests_replayed == 2
    assert report.rows_replayed == 2
    assert report.triage_available is True
    assert report.raw_query_available is True
    assert report.warm_history_available is True
    assert report.full_rebuild_complete is True
    assert report.errors == ()


def test_recovery_reports_partial_availability_when_warm_tier_fails() -> None:
    recent, recent_bytes = _manifest(
        "recent", NOW - timedelta(hours=1), NOW - timedelta(minutes=59), ({"event_id": "1"},)
    )
    warm, warm_bytes = _manifest(
        "warm", NOW - timedelta(days=10), NOW - timedelta(days=10) + timedelta(minutes=1), ({"event_id": "2"},)
    )
    store = InMemoryObjectStore()
    archive = ArchivePublication(store)
    archive.publish(recent, recent_bytes)
    archive.publish(warm, warm_bytes)
    catalog = _Catalog((recent, warm))
    publication = ClickHousePublication(InMemoryBatchManifestStore(), ReplayClient(fail_on={"warm"}))

    report = rebuild_serving_state_from_fos(catalog, archive, publication, service_id="svc", domain="request", now=NOW)

    assert report.triage_available is True
    assert report.raw_query_available is True
    assert report.warm_history_available is False
    assert report.full_rebuild_complete is False
    assert len(report.errors) == 1
    assert "warm" in report.errors[0]


def test_recovery_reports_no_triage_when_recent_tier_fails() -> None:
    recent, recent_bytes = _manifest(
        "recent", NOW - timedelta(hours=1), NOW - timedelta(minutes=59), ({"event_id": "1"},)
    )
    store = InMemoryObjectStore()
    archive = ArchivePublication(store)
    archive.publish(recent, recent_bytes)
    catalog = _Catalog((recent,))
    publication = ClickHousePublication(InMemoryBatchManifestStore(), ReplayClient(fail_on={"recent"}))

    report = rebuild_serving_state_from_fos(catalog, archive, publication, service_id="svc", domain="request", now=NOW)

    assert report.triage_available is False
    assert report.raw_query_available is False
    assert report.full_rebuild_complete is False


def test_recovery_reinserts_rows_whose_manifest_was_already_replayed_once() -> None:
    """Reproduces the live-drill finding: a manifest replayed once before
    (e.g. an earlier partial recovery attempt) is durably marked visible in
    the manifest store. If ClickHouse data is lost again after that, a
    second rebuild must not silently treat the stale visibility record as
    proof the rows are still there — it must actually re-insert them."""
    from backend.high_scale.replay import replay_manifest

    recent, recent_bytes = _manifest(
        "recent", NOW - timedelta(hours=1), NOW - timedelta(minutes=59), ({"event_id": "1"},)
    )
    store = InMemoryObjectStore()
    archive = ArchivePublication(store)
    archive.publish(recent, recent_bytes)
    catalog = _Catalog((recent,))
    manifests = InMemoryBatchManifestStore()
    client = ReplayClient()
    publication = ClickHousePublication(manifests, client)

    # Simulate an earlier, already-completed replay of this exact manifest
    # under the default (non-rebuild-scoped) batch id.
    first = replay_manifest(recent, archive, publication)
    assert first.publication.duplicate is False
    assert client.insert_calls == ["replay:recent"]

    report = rebuild_serving_state_from_fos(catalog, archive, publication, service_id="svc", domain="request", now=NOW)

    assert report.rows_replayed == 1
    assert report.manifests_replayed == 1
    assert report.full_rebuild_complete is True
    # The real assertion: the rebuild must issue a FRESH insert rather than
    # trusting the stale "already visible" record from the earlier replay —
    # otherwise data lost from ClickHouse after that first replay would
    # never actually come back.
    assert len(client.insert_calls) == 2
    assert client.insert_calls[0] != client.insert_calls[1]


def test_resuming_a_rebuild_with_the_same_rebuild_id_stays_idempotent() -> None:
    """A rebuild retried with its OWN rebuild_id (resuming after a crash
    mid-rebuild, say) must NOT duplicate rows already inserted this
    attempt — only a rebuild_id-less (genuinely new) call should force a
    fresh insert."""
    recent, recent_bytes = _manifest(
        "recent", NOW - timedelta(hours=1), NOW - timedelta(minutes=59), ({"event_id": "1"},)
    )
    store = InMemoryObjectStore()
    archive = ArchivePublication(store)
    archive.publish(recent, recent_bytes)
    catalog = _Catalog((recent,))
    client = ReplayClient()
    publication = ClickHousePublication(InMemoryBatchManifestStore(), client)

    first = rebuild_serving_state_from_fos(catalog, archive, publication, service_id="svc", domain="request", now=NOW)
    assert len(client.insert_calls) == 1

    second = rebuild_serving_state_from_fos(
        catalog, archive, publication, service_id="svc", domain="request", now=NOW, rebuild_id=first.rebuild_id
    )

    assert len(client.insert_calls) == 1
    assert second.manifests_replayed == 1
    assert second.rebuild_id == first.rebuild_id


def test_recovery_with_no_archived_history_is_a_trivially_complete_rebuild() -> None:
    catalog = _Catalog(())
    store = InMemoryObjectStore()
    archive = ArchivePublication(store)
    publication = ClickHousePublication(InMemoryBatchManifestStore(), ReplayClient())

    report = rebuild_serving_state_from_fos(catalog, archive, publication, service_id="svc", domain="request", now=NOW)

    assert report.manifests_replayed == 0
    assert report.full_rebuild_complete is True
    assert report.ingest_accepted is True


def test_origin_projection_rebuild_is_resumable_without_replaying_request_facts() -> None:
    recent, recent_bytes = _manifest(
        "recent-origin",
        NOW - timedelta(hours=1),
        NOW - timedelta(minutes=59),
        (
            {
                "timestamp": (NOW - timedelta(hours=1)).isoformat(),
                "url": "/slow",
                "ottfb": 2500,
                "ost": 503,
            },
        ),
    )
    store = InMemoryObjectStore()
    archive = ArchivePublication(store)
    archive.publish(recent, recent_bytes)
    catalog = _Catalog((recent,))
    client = ReplayClient()
    publication = ClickHousePublication(InMemoryBatchManifestStore(), client)

    first = rebuild_origin_projections_from_fos(
        catalog,
        archive,
        publication,
        service_id="svc",
        now=NOW,
    )
    second = rebuild_origin_projections_from_fos(
        catalog,
        archive,
        publication,
        service_id="svc",
        now=NOW,
        rebuild_id=first.rebuild_id,
    )

    assert first.manifests_replayed == second.manifests_replayed == 1
    assert first.rows_replayed == second.rows_replayed == 3
    assert client.insert_calls == [
        f"rebuild-origin:{first.rebuild_id}:recent-origin:summary",
        f"rebuild-origin:{first.rebuild_id}:recent-origin:dimensions",
    ]
