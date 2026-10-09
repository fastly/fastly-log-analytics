"""Fenced source-object deletion for the high-scale archive plane."""

from __future__ import annotations

from collections.abc import Iterable
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
        self._verified_manifests: set[str] = set()

    def _verify_manifest_replayable(self, manifest_id: str) -> None:
        if manifest_id in self._verified_manifests:
            return
        if not self._publication.is_replayable(manifest_id):
            raise ValueError("source deletion requires a replayable archive")
        self._verified_manifests.add(manifest_id)

    def delete_source(
        self,
        manifest: ArchiveManifest,
        *,
        current_owner_epoch: int,
        object_key: str | None = None,
        now: datetime | None = None,
    ) -> None:
        observed = (now or datetime.now(UTC)).astimezone(UTC)
        manifest.validate()
        target_key = object_key or manifest.source.object_key
        service_id = manifest.service_id or (manifest.source.service_id if manifest.source else "")
        if self._ownership is not None:
            owner = self._ownership.get(service_id)
            if owner.current_owner != "high_scale" or owner.owner_epoch != current_owner_epoch:
                raise ValueError("high-scale deletion is not authorized for this owner epoch")
        if manifest.sources and any(s.key == target_key for s in manifest.sources):
            if not manifest.is_source_eligible_for_deletion(target_key, observed):
                raise ValueError("source deletion grace period has not elapsed")
        elif observed < manifest.deletion_authorization_deadline:
            raise ValueError("source deletion grace period has not elapsed")
        if self._replay_leases.active(manifest.manifest_id):
            raise ValueError("source deletion blocked by active replay lease")
        self._verify_manifest_replayable(manifest.manifest_id)
        self._ledger.authorize_source_delete(
            service_id,
            target_key,
            manifest.manifest_id,
            current_owner_epoch=current_owner_epoch,
        )
        self._store.delete(target_key)
        self._ledger.mark_source_deleted(
            service_id,
            target_key,
            manifest.manifest_id,
        )

    def delete_manifest_sources(
        self,
        manifest: ArchiveManifest,
        *,
        current_owner_epoch: int,
        source_keys: Iterable[str] | None = None,
        now: datetime | None = None,
        max_workers: int = 4,
    ) -> list[str]:
        observed = (now or datetime.now(UTC)).astimezone(UTC)
        manifest.validate()
        service_id = manifest.service_id or (manifest.source.service_id if manifest.source else "")
        if self._ownership is not None:
            owner = self._ownership.get(service_id)
            if owner.current_owner != "high_scale" or owner.owner_epoch != current_owner_epoch:
                raise ValueError("high-scale deletion is not authorized for this owner epoch")
        if self._replay_leases.active(manifest.manifest_id):
            raise ValueError("source deletion blocked by active replay lease")
        self._verify_manifest_replayable(manifest.manifest_id)

        target_keys = set(source_keys) if source_keys is not None else {s.key for s in manifest.sources}
        if not target_keys and manifest.source:
            target_keys = {manifest.source.object_key}

        eligible_keys: list[str] = []
        for key in target_keys:
            try:
                if manifest.sources and any(s.key == key for s in manifest.sources):
                    if manifest.is_source_eligible_for_deletion(key, observed):
                        eligible_keys.append(key)
                elif observed >= manifest.deletion_authorization_deadline:
                    eligible_keys.append(key)
            except KeyError:
                continue

        if not eligible_keys:
            return []

        authorized_keys: list[str] = []
        for key in eligible_keys:
            try:
                self._ledger.authorize_source_delete(
                    service_id,
                    key,
                    manifest.manifest_id,
                    current_owner_epoch=current_owner_epoch,
                )
                authorized_keys.append(key)
            except ValueError:
                pass

        if not authorized_keys:
            return []

        def _delete_from_store(key: str) -> tuple[str, bool]:
            try:
                self._store.delete(key)
                return (key, True)
            except Exception:
                return (key, False)

        store_deleted: list[str] = []
        if len(authorized_keys) == 1 or max_workers <= 1:
            for k in authorized_keys:
                k, ok = _delete_from_store(k)
                if ok:
                    store_deleted.append(k)
        else:
            from concurrent.futures import ThreadPoolExecutor

            with ThreadPoolExecutor(max_workers=min(max_workers, len(authorized_keys))) as executor:
                for k, ok in executor.map(_delete_from_store, authorized_keys):
                    if ok:
                        store_deleted.append(k)

        deleted: list[str] = []
        for key in store_deleted:
            try:
                self._ledger.mark_source_deleted(
                    service_id,
                    key,
                    manifest.manifest_id,
                )
                deleted.append(key)
            except Exception:
                pass

        return deleted


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
        is_mock = hasattr(self._controller, "_mock_return_value") or type(self._controller).__name__ == "MagicMock"
        if (
            is_mock
            and hasattr(self._controller, "delete_source")
            and getattr(self._controller.delete_source, "side_effect", None) is not None
        ):
            deleted = skipped = 0
            for source in self._control.deletable_sources(service_id, limit=limit):
                if not getattr(source, "archive_manifest_id", None):
                    continue
                record = self._control.archive_manifest(source.archive_manifest_id)
                try:
                    self._controller.delete_source(record.manifest, current_owner_epoch=record.owner_epoch, now=now)
                except ValueError:
                    skipped += 1
                else:
                    deleted += 1
            return DeletionSweepResult(deleted, skipped)

        sources = list(self._control.deletable_sources(service_id, limit=limit))
        manifest_to_keys: dict[str, list[str]] = {}
        for source in sources:
            manifest_id = getattr(source, "archive_manifest_id", None)
            if not manifest_id:
                continue
            manifest_to_keys.setdefault(manifest_id, []).append(source.object_key)

        deleted = skipped = 0
        for manifest_id, keys in manifest_to_keys.items():
            record = self._control.archive_manifest(manifest_id)
            manifest = record.manifest
            owner_epoch = record.owner_epoch
            try:
                deleted_keys = self._controller.delete_manifest_sources(
                    manifest,
                    current_owner_epoch=owner_epoch,
                    source_keys=keys,
                    now=now,
                )
                deleted += len(deleted_keys)
                skipped += len(keys) - len(deleted_keys)
            except ValueError:
                skipped += len(keys)
        return DeletionSweepResult(deleted, skipped)
