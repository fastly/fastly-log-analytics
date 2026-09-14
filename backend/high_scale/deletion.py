"""Fenced source-object deletion for the high-scale archive plane."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from backend.core.high_scale_contracts import ArchiveState
from backend.high_scale.archive_models import ArchiveManifest
from backend.high_scale.archive_publication import ArchivePublication, ObjectStore
from backend.high_scale.ledger import HighScaleLedger


class ReplayLeaseChecker(Protocol):
    def active(self, manifest_id: str) -> bool: ...


class NoReplayLease:
    def active(self, manifest_id: str) -> bool:
        return False


class OwnerRecord(Protocol):
    current_owner: str
    owner_epoch: int


class OwnershipChecker(Protocol):
    def get(self, service_id: str) -> OwnerRecord: ...


class DeletionLedger(Protocol):
    """The narrow shape ``DeletionController`` needs from a ledger backend.

    ``HighScaleLedger`` (SQLite reference) satisfies this directly;
    ``PostgresDeletionLedger`` below adapts ``PostgresControlPlane`` to it.
    """

    def authorize_source_delete(
        self, service_id: str, object_key: str, archive_manifest_id: str, *, current_owner_epoch: int
    ) -> Any: ...

    def mark_source_deleted(self, service_id: str, object_key: str, archive_manifest_id: str) -> None: ...


class DeletionController:
    def __init__(
        self,
        store: ObjectStore,
        publication: ArchivePublication,
        ledger: DeletionLedger | HighScaleLedger,
        replay_leases: ReplayLeaseChecker | None = None,
        ownership: OwnershipChecker | None = None,
    ) -> None:
        self._store = store
        self._publication = publication
        self._ledger = ledger
        self._replay_leases = replay_leases or NoReplayLease()
        self._ownership = ownership

    def delete_source(
        self,
        manifest: ArchiveManifest,
        *,
        current_owner_epoch: int,
        now: datetime | None = None,
    ) -> None:
        observed = (now or datetime.now(UTC)).astimezone(UTC)
        manifest.validate()
        if self._ownership is not None:
            owner = self._ownership.get(manifest.source.service_id)
            if owner.current_owner != "high_scale" or owner.owner_epoch != current_owner_epoch:
                raise ValueError("high-scale deletion is not authorized for this owner epoch")
        if observed < manifest.deletion_authorization_deadline:
            raise ValueError("source deletion grace period has not elapsed")
        if self._replay_leases.active(manifest.manifest_id):
            raise ValueError("source deletion blocked by active replay lease")
        if not self._publication.is_replayable(manifest.manifest_id):
            raise ValueError("source deletion requires a replayable archive")
        self._ledger.authorize_source_delete(
            manifest.source.service_id,
            manifest.source.object_key,
            manifest.manifest_id,
            current_owner_epoch=current_owner_epoch,
        )
        self._store.delete(manifest.source.object_key)
        self._ledger.mark_source_deleted(
            manifest.source.service_id,
            manifest.source.object_key,
            manifest.manifest_id,
        )


class PostgresDeletionLedger:
    """Adapts ``PostgresControlPlane`` to the ``DeletionLedger`` AND
    ``OwnershipChecker`` shapes ``DeletionController`` needs.

    Postgres's protocol differs from the SQLite reference ledger's in two
    ways ``DeletionController`` doesn't know about: a manifest must be
    promoted ``manifest_committed`` -> ``deletion_eligible`` before
    ``authorize_source_delete`` will accept it, and ``mark_source_deleted``
    needs an owner epoch the ledger-shaped call site never passes. Both are
    handled here so ``DeletionController``'s tested grace-period/replay/
    ownership-fencing logic is reused unchanged against Postgres.

    Also implements ``OwnershipChecker.get`` (``PostgresControlPlane``'s
    equivalent method is named ``owner``, not ``get`` — passing the control
    plane directly as ``ownership=`` fails structurally at the first real
    call, not at construction, since Python has no static Protocol
    enforcement). One instance can be passed as both ``ledger=`` and
    ``ownership=`` to ``DeletionController``.
    """

    def __init__(self, control: Any) -> None:
        self._control = control

    def get(self, service_id: str) -> Any:
        return self._control.owner(service_id)

    def authorize_source_delete(
        self, service_id: str, object_key: str, archive_manifest_id: str, *, current_owner_epoch: int
    ) -> Any:
        try:
            self._control.transition_archive_manifest(
                archive_manifest_id,
                expected_state=ArchiveState.MANIFEST_COMMITTED,
                next_state=ArchiveState.DELETION_ELIGIBLE,
                expected_owner_epoch=current_owner_epoch,
            )
        except ValueError:
            # Already deletion_eligible (a prior sweep tick promoted it, or
            # this is a retry), or a genuine owner-epoch mismatch — either
            # way, the authorize_source_delete call below is the real fence
            # and will raise if the manifest still isn't eligible.
            pass
        return self._control.authorize_source_delete(
            service_id,
            object_key,
            manifest_id=archive_manifest_id,
            expected_owner_epoch=current_owner_epoch,
        )

    def mark_source_deleted(self, service_id: str, object_key: str, archive_manifest_id: str) -> None:
        owner = self._control.owner(service_id)
        self._control.mark_source_deleted(
            service_id,
            object_key,
            manifest_id=archive_manifest_id,
            expected_owner_epoch=owner.owner_epoch,
        )


@dataclass(frozen=True)
class DeletionSweepResult:
    deleted: int
    skipped: int


class HighScaleDeletionSweeper:
    """Periodically deletes raw source objects whose archive is durably
    verified and whose grace period has elapsed.

    Delegates all authorization/state-machine work to ``DeletionController``
    so the tested grace-period/replay-lease/ownership fencing logic isn't
    duplicated here — this class only supplies the candidate list and loops.
    """

    def __init__(self, *, control: Any, controller: DeletionController) -> None:
        self._control = control
        self._controller = controller

    def sweep(self, *, service_id: str, limit: int = 100, now: datetime | None = None) -> DeletionSweepResult:
        deleted = skipped = 0
        for source in self._control.deletable_sources(service_id, limit=limit):
            if not source.archive_manifest_id:
                continue
            record = self._control.archive_manifest(source.archive_manifest_id)
            try:
                self._controller.delete_source(record.manifest, current_owner_epoch=record.owner_epoch, now=now)
            except ValueError:
                skipped += 1
            else:
                deleted += 1
        return DeletionSweepResult(deleted, skipped)
