import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, call

import pytest

from backend.core.high_scale_contracts import ArchiveState
from backend.high_scale.archive_models import ArchiveArtifact, ArchiveManifest, ArchiveSourceObject
from backend.high_scale.archive_publication import ArchivePublication, InMemoryObjectStore
from backend.high_scale.deletion import (
    DeletionController,
    DeletionSweepResult,
    HighScaleDeletionSweeper,
    PostgresDeletionLedger,
)
from backend.high_scale.ledger import HighScaleLedger
from backend.high_scale.ownership import OwnershipStore


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
