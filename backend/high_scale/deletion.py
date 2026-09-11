"""Fenced source-object deletion for the high-scale archive plane."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Protocol

from backend.high_scale.archive_models import ArchiveManifest
from backend.high_scale.archive_publication import ArchivePublication, ObjectStore
from backend.high_scale.ledger import HighScaleLedger


class ReplayLeaseChecker(Protocol):
    def active(self, manifest_id: str) -> bool: ...


class NoReplayLease:
    def active(self, manifest_id: str) -> bool:
        return False


class DeletionController:
    def __init__(
        self,
        store: ObjectStore,
        publication: ArchivePublication,
        ledger: HighScaleLedger,
        replay_leases: ReplayLeaseChecker | None = None,
    ) -> None:
        self._store = store
        self._publication = publication
        self._ledger = ledger
        self._replay_leases = replay_leases or NoReplayLease()

    def delete_source(
        self,
        manifest: ArchiveManifest,
        *,
        current_owner_epoch: int,
        now: datetime | None = None,
    ) -> None:
        observed = (now or datetime.now(UTC)).astimezone(UTC)
        manifest.validate()
        if observed < manifest.deletion_authorization_deadline:
            raise ValueError("source deletion grace period has not elapsed")
        if self._replay_leases.active(manifest.manifest_id):
            raise ValueError("source deletion blocked by active replay lease")
        if not self._publication.is_replayable(manifest.manifest_id):
            raise ValueError("source deletion requires a replayable archive")
        self._ledger.authorize_source_delete(
            manifest.source.object_key,
            manifest.manifest_id,
            current_owner_epoch=current_owner_epoch,
        )
        self._store.delete(manifest.source.object_key)
