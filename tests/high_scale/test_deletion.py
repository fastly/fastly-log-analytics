import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, call

import pytest

from backend.high_scale.archive_models import (
    ArchiveArtifact,
    ArchiveManifest,
    ArchiveSourceEntry,
    ArchiveSourceObject,
    ArchiveState,
)
from backend.high_scale.archive_publication import ArchivePublication, InMemoryObjectStore
from backend.high_scale.deletion import (
    DeletionController,
    DeletionSweepResult,
    HighScaleDeletionSweeper,
    PostgresDeletionLedger,
)
from tests.high_scale.conftest import HighScaleLedger, OwnershipStore


def test_source_deletion_requires_archive_and_owner_fence() -> None:
    store = InMemoryObjectStore()
    publication = ArchivePublication(store)
    ledger = HighScaleLedger()
    try:
        source = ArchiveSourceObject("svc", "request", "raw/request/a.gz", "sha256:source", 4, "v1")
        ledger.discover("svc", "request", source.object_key, source.checksum, size_bytes=4, version="v1")
        claim = ledger.claim(source.service_id, source.object_key, "worker")
        ledger.record_counts(source.service_id, source.object_key, accepted_rows=1, malformed_rows=0)
        ledger.mark_appended(source.service_id, source.object_key, claim.lease_generation)
        now = datetime(2026, 9, 4, tzinfo=UTC)
        manifest = ArchiveManifest(
            "manifest-1",
            source,
            ArchiveArtifact(
                "s3://archive/artifacts/manifest-1.parquet",
                _checksum(b"data"),
                4,
                1,
                4,
                "sha256:events",
                "request.v1",
                "normalize.v1",
            ),
            now - timedelta(days=3),
            now - timedelta(days=2),
            now - timedelta(days=1),
            now - timedelta(hours=1),
            3,
        )
        artifact = b"data"
        store.put(source.object_key, b"raw")
        publication.publish(manifest, artifact)
        ledger.mark_archived(source.service_id, source.object_key, claim.lease_generation, manifest.manifest_id, 3)
        ledger.acknowledge(source.service_id, source.object_key, manifest.manifest_id)

        DeletionController(store, publication, ledger).delete_source(manifest, current_owner_epoch=3, now=now)

        assert not store.exists(source.object_key)
        ledger.mark_source_deleted(source.service_id, source.object_key, manifest.manifest_id)
    finally:
        ledger.close()


def test_source_deletion_is_blocked_before_deadline() -> None:
    store = InMemoryObjectStore()
    publication = ArchivePublication(store)
    ledger = HighScaleLedger()
    try:
        with pytest.raises(ValueError, match="grace period"):
            DeletionController(store, publication, ledger).delete_source(
                _manifest(), current_owner_epoch=3, now=datetime(2026, 9, 1, tzinfo=UTC)
            )
    finally:
        ledger.close()


def test_source_deletion_rejects_non_high_scale_owner() -> None:
    ownership = OwnershipStore()
    ledger = HighScaleLedger()
    ownership.initialize("svc", owner="standard", source_cursor="cursor-0")
    try:
        with pytest.raises(ValueError, match="not authorized"):
            DeletionController(
                InMemoryObjectStore(),
                ArchivePublication(InMemoryObjectStore()),
                ledger,
                ownership=ownership,
            ).delete_source(_manifest(), current_owner_epoch=1)
    finally:
        ownership.close()
        ledger.close()


def _manifest() -> ArchiveManifest:
    source = ArchiveSourceObject("svc", "request", "raw/request/a.gz", "sha256:source", 4, "v1")
    return ArchiveManifest(
        "manifest-1",
        source,
        ArchiveArtifact("s3://archive/artifacts/manifest-1.parquet", "sha256:x", 1, 1, 1, "sha256:e", "v1", "v1"),
        datetime(2026, 9, 1, tzinfo=UTC),
        datetime(2026, 9, 1, tzinfo=UTC),
        datetime(2026, 9, 2, tzinfo=UTC),
        datetime(2026, 9, 3, tzinfo=UTC),
        3,
    )


def _checksum(value: bytes) -> str:
    return f"sha256:{hashlib.sha256(value).hexdigest()}"


# ── PostgresDeletionLedger ──────────────────────────────────────────────────
#
# PostgresControlPlane's authorize_source_delete/mark_source_deleted have a
# different shape than HighScaleLedger's (manifest must already be
# deletion_eligible; mark_source_deleted needs an owner epoch the
# DeletionController call site never passes). This adapter bridges that gap
# so DeletionController's tested grace-period/replay/ownership logic runs
# unchanged against Postgres. These tests pin the adapter's own logic with a
# fake control double — the real Postgres queries are covered separately in
# tests/core/test_high_scale_postgres_control.py.


@dataclass
class _FakeControl:
    owner_epoch_for_owner_call: int = 5
    transition_error: Exception | None = None

    def __post_init__(self) -> None:
        self.transition_calls: list[tuple] = []
        self.authorize_calls: list[tuple] = []
        self.mark_deleted_calls: list[tuple] = []

    def transition_archive_manifest(self, manifest_id, *, expected_state, next_state, expected_owner_epoch):
        self.transition_calls.append((manifest_id, expected_state, next_state, expected_owner_epoch))
        if self.transition_error is not None:
            raise self.transition_error

    def authorize_source_delete(self, service_id, object_key, *, manifest_id, expected_owner_epoch):
        self.authorize_calls.append((service_id, object_key, manifest_id, expected_owner_epoch))
        return "authorized"

    def owner(self, service_id):
        return MagicMock(owner_epoch=self.owner_epoch_for_owner_call)

    def mark_source_deleted(self, service_id, object_key, *, manifest_id, expected_owner_epoch):
        self.mark_deleted_calls.append((service_id, object_key, manifest_id, expected_owner_epoch))


def test_postgres_deletion_ledger_satisfies_the_ownership_checker_protocol() -> None:
    """DeletionController's OwnershipChecker protocol calls ``.get(service_id)``,
    but PostgresControlPlane's equivalent method is named ``.owner(...)`` —
    passing the control plane directly as ``ownership=`` fails at the first
    real call (AttributeError), not at construction. This was caught live
    against the real k8s worker before this test was added."""
    control = _FakeControl()
    adapter = PostgresDeletionLedger(control)

    result = adapter.get("svc")

    assert result.owner_epoch == control.owner_epoch_for_owner_call


def test_postgres_deletion_ledger_promotes_manifest_then_authorizes() -> None:
    control = _FakeControl()
    adapter = PostgresDeletionLedger(control)

    result = adapter.authorize_source_delete("svc", "raw/one.gz", "manifest-1", current_owner_epoch=3)

    assert result == "authorized"
    assert control.transition_calls == [
        ("manifest-1", ArchiveState.MANIFEST_COMMITTED, ArchiveState.DELETION_ELIGIBLE, 3)
    ]
    assert control.authorize_calls == [("svc", "raw/one.gz", "manifest-1", 3)]


def test_postgres_deletion_ledger_authorize_tolerates_an_already_promoted_manifest() -> None:
    """A prior sweep tick (or a race) may have already promoted the
    manifest to deletion_eligible — the promotion attempt then fails with
    ValueError (not an adjacent-state transition), which must not stop the
    real authorization call from proceeding."""
    control = _FakeControl(transition_error=ValueError("archive manifest transition is not adjacent"))
    adapter = PostgresDeletionLedger(control)

    result = adapter.authorize_source_delete("svc", "raw/one.gz", "manifest-1", current_owner_epoch=3)

    assert result == "authorized"
    assert control.authorize_calls == [("svc", "raw/one.gz", "manifest-1", 3)]


def test_postgres_deletion_ledger_mark_deleted_resolves_owner_epoch_itself() -> None:
    """DeletionController's ledger-shaped call site never passes an owner
    epoch to mark_source_deleted — the adapter must resolve one itself."""
    control = _FakeControl(owner_epoch_for_owner_call=7)
    adapter = PostgresDeletionLedger(control)

    adapter.mark_source_deleted("svc", "raw/one.gz", "manifest-1")

    assert control.mark_deleted_calls == [("svc", "raw/one.gz", "manifest-1", 7)]


# ── HighScaleDeletionSweeper ─────────────────────────────────────────────────


@dataclass
class _ManifestRecord:
    manifest: object
    owner_epoch: int


class _SweepControl:
    def __init__(self, sources, manifests) -> None:
        self._sources = sources
        self._manifests = manifests

    def deletable_sources(self, service_id, *, limit):
        return self._sources

    def archive_manifest(self, manifest_id):
        return self._manifests[manifest_id]


def test_sweeper_deletes_eligible_sources_and_skips_ineligible_ones() -> None:
    """The sweeper itself does no grace-period math — DeletionController
    does, and its own tests cover that. Here we pin only the sweeper's
    looping/counting/defensive behavior against a mocked controller."""
    now = datetime(2026, 9, 5, tzinfo=UTC)
    first = _manifest_for("first", now=now)
    second = _manifest_for("second", now=now)
    control = _SweepControl(
        sources=[
            MagicMock(archive_manifest_id="first"),
            MagicMock(archive_manifest_id="second"),
            MagicMock(archive_manifest_id=None),  # defensive: no manifest yet, must be skipped not crashed
        ],
        manifests={
            "first": _ManifestRecord(first, owner_epoch=3),
            "second": _ManifestRecord(second, owner_epoch=3),
        },
    )
    controller = MagicMock()
    controller.delete_source.side_effect = [None, ValueError("source deletion grace period has not elapsed")]
    sweeper = HighScaleDeletionSweeper(control=control, controller=controller)

    result = sweeper.sweep(service_id="svc", now=now)

    assert result == DeletionSweepResult(deleted=1, skipped=1)
    assert controller.delete_source.call_args_list == [
        call(first, current_owner_epoch=3, now=now),
        call(second, current_owner_epoch=3, now=now),
    ]


def _manifest_for(manifest_id: str, *, now: datetime) -> ArchiveManifest:
    source = ArchiveSourceObject("svc", "request", f"raw/{manifest_id}.gz", "sha256:source", 4, "v1")
    return ArchiveManifest(
        manifest_id,
        source,
        ArchiveArtifact(f"s3://archive/{manifest_id}.parquet", "sha256:x", 1, 1, 1, "sha256:e", "v1", "v1"),
        now - timedelta(days=3),
        now - timedelta(days=2),
        now - timedelta(days=1),
        now - timedelta(hours=1),
        3,
    )


def test_delete_manifest_sources_multi_source() -> None:
    store = InMemoryObjectStore()
    publication = ArchivePublication(store)
    ledger = HighScaleLedger()
    try:
        now = datetime(2026, 9, 10, tzinfo=UTC)
        sources = [
            ArchiveSourceEntry(
                key=f"raw/request/part_{i}.gz",
                checksum=f"sha256:source_{i}",
                size_bytes=10,
                row_start=i * 5,
                row_count=5,
                deletion_deadline=now - timedelta(hours=1) if i < 2 else now + timedelta(hours=1),
            )
            for i in range(3)
        ]
        manifest = ArchiveManifest(
            "manifest-batch",
            artifact=ArchiveArtifact(
                "s3://archive/artifacts/manifest-batch.parquet",
                _checksum(b"batch_data"),
                10,
                15,
                10,
                "sha256:events",
                "request.v1",
                "normalize.v1",
            ),
            coverage_start=now - timedelta(days=3),
            coverage_end=now - timedelta(days=2),
            retention_deadline=now - timedelta(days=1),
            deletion_authorization_deadline=now - timedelta(hours=1),
            archive_epoch=3,
            service_id="svc",
            domain="request",
            sources=sources,
        )

        for s in sources:
            store.put(s.key, b"raw_gz")
            ledger.discover("svc", "request", s.key, s.checksum, size_bytes=s.size_bytes)
            claim = ledger.claim("svc", s.key, "worker")
            ledger.record_counts("svc", s.key, accepted_rows=s.row_count, malformed_rows=0)
            ledger.mark_appended("svc", s.key, claim.lease_generation)
            ledger.mark_archived("svc", s.key, claim.lease_generation, manifest.manifest_id, 3)
            ledger.acknowledge("svc", s.key, manifest.manifest_id)

        publication.publish(manifest, b"batch_data")

        controller = DeletionController(store, publication, ledger)
        deleted = controller.delete_manifest_sources(manifest, current_owner_epoch=3, now=now)

        # First 2 sources had deletion_deadline in past -> deleted
        assert set(deleted) == {"raw/request/part_0.gz", "raw/request/part_1.gz"}
        assert not store.exists("raw/request/part_0.gz")
        assert not store.exists("raw/request/part_1.gz")
        # 3rd source had deletion_deadline in future -> preserved
        assert store.exists("raw/request/part_2.gz")
    finally:
        ledger.close()


def test_sweeper_groups_by_manifest_and_deletes_batch() -> None:
    now = datetime(2026, 9, 10, tzinfo=UTC)
    manifest = _manifest_for("m1", now=now)
    control = MagicMock()
    control.deletable_sources.return_value = [
        MagicMock(archive_manifest_id="m1", object_key="raw/1.gz"),
        MagicMock(archive_manifest_id="m1", object_key="raw/2.gz"),
    ]
    control.archive_manifest.return_value = MagicMock(manifest=manifest, owner_epoch=3)

    controller = MagicMock(spec=DeletionController)
    controller.delete_manifest_sources.return_value = ["raw/1.gz", "raw/2.gz"]

    sweeper = HighScaleDeletionSweeper(control=control, controller=controller)
    result = sweeper.sweep(service_id="svc", now=now)

    assert result == DeletionSweepResult(deleted=2, skipped=0)
    controller.delete_manifest_sources.assert_called_once_with(
        manifest,
        current_owner_epoch=3,
        source_keys=["raw/1.gz", "raw/2.gz"],
        now=now,
    )


# ── Merged from test_archive_only_recovery.py ────────────────────────

import tempfile
from dataclasses import replace
from pathlib import Path
from typing import Any

from backend.high_scale.archive_writer import write_batch_archive_checkpoint
from backend.high_scale.replay import replay_manifest, replay_origin_projections

from backend.high_scale.publication import (
    ClickHousePublication,
    HighScaleBatch,
    InMemoryBatchManifestStore,
    InsertReceipt,
    PublicationStatus,
)
from backend.high_scale.recovery import (
    rebuild_origin_projections_from_fos,
    rebuild_serving_state_from_fos,
)


class _MockClickHouseClient:
    def __init__(self) -> None:
        self.batches: list[HighScaleBatch] = []

    def insert(self, batch: HighScaleBatch) -> InsertReceipt:
        self.batches.append(batch)
        return InsertReceipt(batch.batch_id, len(batch.rows), batch.digest)

    @property
    def request_rows(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for b in self.batches:
            if b.domain == "request":
                rows.extend(b.rows)
        return rows


class _InMemoryCatalog:
    def __init__(self, manifests: tuple[ArchiveManifest, ...]) -> None:
        self._manifests = manifests

    def manifests_covering(
        self, service_id: str, domain: str, start: datetime, end: datetime
    ) -> tuple[ArchiveManifest, ...]:
        return tuple(
            m
            for m in self._manifests
            if m.service_id == service_id and m.domain == domain and m.coverage_end > start and m.coverage_start < end
        )


def _build_synthetic_batch_archive(
    service_id: str,
    output_dir: Path,
    source_count: int = 5,
    rows_per_source: int = 5,
    now: datetime | None = None,
) -> tuple[ArchiveManifest, bytes, list[dict[str, Any]]]:
    observed = now or datetime(2026, 10, 9, 12, 0, tzinfo=UTC)
    source_batches: list[tuple[ArchiveSourceObject, tuple[dict[str, Any], ...]]] = []
    all_events: list[dict[str, Any]] = []

    for s_idx in range(source_count):
        events_for_source: list[dict[str, Any]] = []
        for r_idx in range(rows_per_source):
            event = {
                "event_id": f"evt-{service_id}-{s_idx}-{r_idx}",
                "timestamp": (observed - timedelta(minutes=s_idx, seconds=r_idx * 10)).isoformat(),
                "client_ip": f"198.51.100.{(s_idx * 5 + r_idx) % 250 + 1}",
                "request_method": "GET" if r_idx % 2 == 0 else "POST",
                "url": f"/api/v1/item/{s_idx}_{r_idx}",
                "status": 200 if r_idx != 3 else 500,
                "ost": 200 if r_idx != 3 else 500,
                "ottfb": 45000.0,
                "ttfb": 0.045,
                "cache": "MISS",
                "resp_bytes": 1024 * (r_idx + 1),
                "pop": "JFK" if s_idx % 2 == 0 else "SFO",
                "fastly_backend_name": "origin-primary",
                "time_elapsed": 30000 + (r_idx * 500),
            }
            events_for_source.append(event)
            all_events.append(event)

        source_obj = ArchiveSourceObject(
            service_id=service_id,
            domain="request",
            object_key=f"raw/request/year=2026/month=10/day=09/source_{s_idx:03d}.gz",
            checksum=f"sha256:synth_{s_idx:04d}",
            size_bytes=512,
            version=f"v{s_idx}",
        )
        source_batches.append((source_obj, tuple(events_for_source)))

    manifest = write_batch_archive_checkpoint(
        output_dir,
        service_id=service_id,
        domain="request",
        source_batches=source_batches,
        archive_epoch=1,
        schema_version="request.v1",
        transform_version="normalize.v1",
        coverage_start=observed - timedelta(hours=1),
        coverage_end=observed,
        retention_seconds=86400 * 30,
        deletion_grace_seconds=86400 * 7,
    )

    artifact_path = Path(manifest.artifact.uri.removeprefix("file://"))
    artifact_bytes = artifact_path.read_bytes()
    normalized_manifest = replace(
        manifest,
        artifact=replace(
            manifest.artifact,
            uri=f"s3://archive/artifacts/{manifest.manifest_id}.parquet",
        ),
    )
    return normalized_manifest, artifact_bytes, all_events


def test_archive_only_recovery_rebuilds_all_events_without_raw_logs() -> None:
    """Verifies that a lost ClickHouse table can be completely rebuilt directly
    from FOS archive artifacts using the batch manifest format, without touching
    or requiring the original raw Fastly .gz log files.
    """
    service_id = "recover-svc-001"
    now = datetime(2026, 10, 9, 14, 0, tzinfo=UTC)

    with tempfile.TemporaryDirectory() as tmp_dir:
        manifest, artifact_bytes, expected_events = _build_synthetic_batch_archive(
            service_id=service_id,
            output_dir=Path(tmp_dir),
            source_count=5,
            rows_per_source=5,
            now=now,
        )

    # 1. Publish to FOS archive store
    object_store = InMemoryObjectStore()
    archive = ArchivePublication(object_store)
    pub_res = archive.publish(manifest, artifact_bytes)
    assert archive.is_replayable(manifest.manifest_id)

    # 2. Simulate complete absence/deletion of edge raw logs
    # No raw .gz files exist or are referenced anywhere in the recovery path.

    # 3. Simulate a fresh ClickHouse cluster after state wipe
    ch_client = _MockClickHouseClient()
    manifest_store = InMemoryBatchManifestStore()
    publication = ClickHousePublication(manifest_store, ch_client)

    # 4. Replay the batch manifest
    replay_res = replay_manifest(manifest, archive, publication)

    assert replay_res.manifest_id == manifest.manifest_id
    assert replay_res.rows_read == 25
    assert replay_res.publication.rows_visible == 25
    assert replay_res.publication.status == PublicationStatus.VISIBLE
    assert replay_res.publication.duplicate is False

    # 5. Assert all 25 rows recovered with accurate attributes
    recovered_rows = ch_client.request_rows
    assert len(recovered_rows) == 25

    recovered_ids = {r["event_id"] for r in recovered_rows}
    expected_ids = {e["event_id"] for e in expected_events}
    assert recovered_ids == expected_ids


def test_archive_only_recovery_rebuilds_origin_projections() -> None:
    """Verifies that origin projections (summary and dimensions) can be fully
    rebuilt from an archived batch manifest.
    """
    service_id = "recover-svc-002"
    now = datetime(2026, 10, 9, 14, 30, tzinfo=UTC)

    with tempfile.TemporaryDirectory() as tmp_dir:
        manifest, artifact_bytes, _ = _build_synthetic_batch_archive(
            service_id=service_id,
            output_dir=Path(tmp_dir),
            source_count=5,
            rows_per_source=5,
            now=now,
        )

    object_store = InMemoryObjectStore()
    archive = ArchivePublication(object_store)
    archive.publish(manifest, artifact_bytes)

    ch_client = _MockClickHouseClient()
    manifest_store = InMemoryBatchManifestStore()
    publication = ClickHousePublication(manifest_store, ch_client)

    proj_res = replay_origin_projections(
        manifest,
        archive,
        publication,
        rebuild_id="drill-origin-001",
    )

    assert proj_res.rows_read == 25
    assert proj_res.rows_published > 0

    origin_batches = [b for b in ch_client.batches if b.domain in {"origin_summary", "origin_dimensions"}]
    assert len(origin_batches) >= 1
    total_proj_rows = sum(len(b.rows) for b in origin_batches)
    assert total_proj_rows == proj_res.rows_published


def test_archive_only_recovery_idempotency_and_rebuild_scopes() -> None:
    """Verifies that:
    1. A duplicate replay with default batch ID is correctly recognized as duplicate.
    2. A disaster-recovery replay scoped with a fresh rebuild ID successfully re-inserts.
    """
    service_id = "recover-svc-003"
    now = datetime(2026, 10, 9, 15, 0, tzinfo=UTC)

    with tempfile.TemporaryDirectory() as tmp_dir:
        manifest, artifact_bytes, _ = _build_synthetic_batch_archive(
            service_id=service_id,
            output_dir=Path(tmp_dir),
            source_count=5,
            rows_per_source=5,
            now=now,
        )

    object_store = InMemoryObjectStore()
    archive = ArchivePublication(object_store)
    archive.publish(manifest, artifact_bytes)

    inserted_batches: list[HighScaleBatch] = []

    class CountingClient:
        def insert(self, batch: HighScaleBatch) -> InsertReceipt:
            inserted_batches.append(batch)
            return InsertReceipt(batch.batch_id, len(batch.rows), batch.digest)

    manifest_store = InMemoryBatchManifestStore()
    publication = ClickHousePublication(manifest_store, CountingClient())

    # First attempt: Fresh insert
    res1 = replay_manifest(manifest, archive, publication)
    assert res1.publication.duplicate is False
    assert len(inserted_batches) == 1

    # Second attempt: Duplicate with default batch ID
    res2 = replay_manifest(manifest, archive, publication)
    assert res2.publication.duplicate is True
    assert len(inserted_batches) == 1

    # Third attempt: Rebuild-scoped batch ID (disaster recovery after volume wipe)
    rebuild_batch_id = f"rebuild:disaster-drill-99:{manifest.manifest_id}"
    res3 = replay_manifest(manifest, archive, publication, batch_id=rebuild_batch_id)
    assert res3.publication.duplicate is False
    assert len(inserted_batches) == 2


def test_archive_only_recovery_catalog_tiered_rebuild() -> None:
    """Verifies full catalog recovery across recent and warm history tiers
    using batch manifests and rebuild_serving_state_from_fos.
    """
    service_id = "recover-svc-004"
    now = datetime(2026, 10, 9, 16, 0, tzinfo=UTC)

    with tempfile.TemporaryDirectory() as tmp_dir:
        recent_manifest, recent_bytes, _ = _build_synthetic_batch_archive(
            service_id=service_id,
            output_dir=Path(tmp_dir),
            source_count=3,
            rows_per_source=10,
            now=now - timedelta(hours=2),
        )
        warm_manifest, warm_bytes, _ = _build_synthetic_batch_archive(
            service_id=service_id,
            output_dir=Path(tmp_dir),
            source_count=4,
            rows_per_source=10,
            now=now - timedelta(days=5),
        )

    object_store = InMemoryObjectStore()
    archive = ArchivePublication(object_store)
    archive.publish(recent_manifest, recent_bytes)
    archive.publish(warm_manifest, warm_bytes)

    catalog = _InMemoryCatalog((recent_manifest, warm_manifest))
    ch_client = _MockClickHouseClient()
    manifest_store = InMemoryBatchManifestStore()
    publication = ClickHousePublication(manifest_store, ch_client)

    report = rebuild_serving_state_from_fos(
        catalog,
        archive,
        publication,
        service_id=service_id,
        domain="request",
        now=now,
    )

    assert report.full_rebuild_complete is True
    assert report.manifests_replayed == 2
    assert report.rows_replayed == 70  # (3*10) + (4*10)
    assert report.triage_available is True
    assert report.raw_query_available is True
    assert report.warm_history_available is True
    assert len(report.errors) == 0

    # Also test origin projection catalog rebuild
    proj_report = rebuild_origin_projections_from_fos(
        catalog,
        archive,
        publication,
        service_id=service_id,
        now=now,
    )
    assert proj_report.full_rebuild_complete is True
    assert proj_report.manifests_replayed == 2
    assert proj_report.rows_replayed > 0
