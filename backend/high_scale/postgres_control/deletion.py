"""Transactional Postgres control-plane state for the isolated high-scale slice."""

from __future__ import annotations

import contextlib
import logging
from collections.abc import Callable, Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from backend import config
from backend.high_scale.archive import (
    ArchiveManifest,
    ArchivePublication,
    ArchiveState,
    ObjectStore,
)

logger = logging.getLogger("backend.high_scale.postgres_control")
from backend.high_scale.postgres_control.control_plane import _observed_utc


@dataclass(frozen=True)
class DeletionAuthorization:
    object_id: str
    manifest_id: str
    owner_epoch: int
    authorized_at: datetime


# ── Fenced Source-Object Deletion (folded from deletion.py) ───────────────────
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
    """The narrow shape ``DeletionController`` needs from a ledger backend."""

    def authorize_source_delete(
        self, service_id: str, object_key: str, archive_manifest_id: str, *, current_owner_epoch: int
    ) -> Any: ...
    def mark_source_deleted(self, service_id: str, object_key: str, archive_manifest_id: str) -> None: ...


class DeletionController:
    def __init__(
        self,
        store: ObjectStore,
        publication: ArchivePublication,
        ledger: DeletionLedger,
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
        if len(self._verified_manifests) >= 4096:
            self._verified_manifests.clear()
        self._verified_manifests.add(manifest_id)

    def _check_delete_fences(self, manifest: ArchiveManifest, service_id: str, current_owner_epoch: int) -> None:
        if self._ownership is not None:
            owner = self._ownership.get(service_id)
            if owner.current_owner != "high_scale" or owner.owner_epoch != current_owner_epoch:
                raise ValueError("high-scale deletion is not authorized for this owner epoch")
        if self._replay_leases.active(manifest.manifest_id):
            raise ValueError("source deletion blocked by active replay lease")
        self._verify_manifest_replayable(manifest.manifest_id)

    @staticmethod
    def _is_key_eligible(manifest: ArchiveManifest, key: str, observed: datetime) -> bool:
        if manifest.sources and any(s.key == key for s in manifest.sources):
            return manifest.is_source_eligible_for_deletion(key, observed)
        return observed >= manifest.deletion_authorization_deadline

    def _mark_ledger_deleted(self, service_id: str, key: str, manifest_id: str, current_owner_epoch: int) -> None:
        try:
            self._ledger.mark_source_deleted(
                service_id,
                key,
                manifest_id,
                current_owner_epoch=current_owner_epoch,  # type: ignore[call-arg]
            )
        except TypeError:
            self._ledger.mark_source_deleted(service_id, key, manifest_id)

    def delete_source(
        self,
        manifest: ArchiveManifest,
        *,
        current_owner_epoch: int,
        object_key: str | None = None,
        now: datetime | None = None,
    ) -> None:
        observed = _observed_utc(now)
        manifest.validate()
        target_key = object_key or (manifest.source.object_key if manifest.source else manifest.sources[0].key)
        service_id = manifest.service_id or (manifest.source.service_id if manifest.source else "")
        if self._ownership is not None:
            owner = self._ownership.get(service_id)
            if owner.current_owner != "high_scale" or owner.owner_epoch != current_owner_epoch:
                raise ValueError("high-scale deletion is not authorized for this owner epoch")
        if not self._is_key_eligible(manifest, target_key, observed):
            raise ValueError("source deletion grace period has not elapsed")
        if self._replay_leases.active(manifest.manifest_id):
            raise ValueError("source deletion blocked by active replay lease")
        self._verify_manifest_replayable(manifest.manifest_id)
        self._ledger.authorize_source_delete(
            service_id, target_key, manifest.manifest_id, current_owner_epoch=current_owner_epoch
        )
        self._store.delete(target_key)
        self._mark_ledger_deleted(service_id, target_key, manifest.manifest_id, current_owner_epoch)
        if not manifest.sources:
            self._verified_manifests.discard(manifest.manifest_id)

    def delete_manifest_sources(
        self,
        manifest: ArchiveManifest,
        *,
        current_owner_epoch: int,
        source_keys: Iterable[str] | None = None,
        now: datetime | None = None,
        max_workers: int = 4,
    ) -> list[str]:
        observed = _observed_utc(now)
        manifest.validate()
        service_id = manifest.service_id or (manifest.source.service_id if manifest.source else "")
        self._check_delete_fences(manifest, service_id, current_owner_epoch)
        target_keys = set(source_keys) if source_keys is not None else {s.key for s in manifest.sources}
        if not target_keys and manifest.source:
            target_keys = {manifest.source.object_key}
        eligible_keys: list[str] = []
        for key in target_keys:
            with contextlib.suppress(KeyError):
                if self._is_key_eligible(manifest, key, observed):
                    eligible_keys.append(key)
        if not eligible_keys:
            return []
        authorized_keys: list[str] = []
        for key in eligible_keys:
            with contextlib.suppress(ValueError):
                self._ledger.authorize_source_delete(
                    service_id, key, manifest.manifest_id, current_owner_epoch=current_owner_epoch
                )
                authorized_keys.append(key)
        if not authorized_keys:
            return []

        def _delete_from_store(key: str) -> tuple[str, bool]:
            try:
                self._store.delete(key)
                return (key, True)
            except Exception:
                return (key, False)

        if len(authorized_keys) == 1 or max_workers <= 1:
            results = [_delete_from_store(k) for k in authorized_keys]
        else:
            with ThreadPoolExecutor(max_workers=min(max_workers, len(authorized_keys))) as executor:
                results = list(executor.map(_delete_from_store, authorized_keys))
        store_deleted = [k for k, ok in results if ok]
        deleted: list[str] = []
        for key in store_deleted:
            with contextlib.suppress(Exception):
                self._mark_ledger_deleted(service_id, key, manifest.manifest_id, current_owner_epoch)
                deleted.append(key)
        if source_keys is None and len(deleted) >= len(target_keys):
            self._verified_manifests.discard(manifest.manifest_id)
        return deleted


class PostgresDeletionLedger:
    """Adapts ``PostgresControlPlane`` to the ``DeletionLedger`` AND ``OwnershipChecker`` shapes."""

    def __init__(self, control: Any) -> None:
        self._control = control
        self._promoted_manifests: set[str] = set()

    def get(self, service_id: str) -> Any:
        return self._control.owner(service_id)

    def authorize_source_delete(
        self, service_id: str, object_key: str, archive_manifest_id: str, *, current_owner_epoch: int
    ) -> Any:
        if archive_manifest_id not in self._promoted_manifests:
            with contextlib.suppress(ValueError):
                self._control.transition_archive_manifest(
                    archive_manifest_id,
                    expected_state=ArchiveState.MANIFEST_COMMITTED,
                    next_state=ArchiveState.DELETION_ELIGIBLE,
                    expected_owner_epoch=current_owner_epoch,
                )
            if len(self._promoted_manifests) >= 4096:
                self._promoted_manifests.clear()
            self._promoted_manifests.add(archive_manifest_id)
        return self._control.authorize_source_delete(
            service_id, object_key, manifest_id=archive_manifest_id, expected_owner_epoch=current_owner_epoch
        )

    def mark_source_deleted(
        self, service_id: str, object_key: str, archive_manifest_id: str, *, current_owner_epoch: int | None = None
    ) -> None:
        owner_epoch = (
            current_owner_epoch if current_owner_epoch is not None else self._control.owner(service_id).owner_epoch
        )
        self._control.mark_source_deleted(
            service_id, object_key, manifest_id=archive_manifest_id, expected_owner_epoch=owner_epoch
        )


@dataclass(frozen=True)
class DeletionSweepResult:
    deleted: int
    skipped: int


class HighScaleArchiveExpirer:
    """Deletes archive artifacts once their data is past the service's retention (ClickHouse has expired it too).

    The archive is the only copy that can rebuild ClickHouse after a volume loss, because raw source objects are
    deleted once archived, so it is kept for the retention window and then removed. ``data_retention_days`` covers
    the request domain and ``rum_retention_days`` the RUM domains; 0 keeps a domain forever. A day of margin keeps
    the archive until after the matching ClickHouse TTL.
    """

    MARGIN_DAYS = 1

    def __init__(
        self,
        *,
        control: Any,
        archive: ArchivePublication,
        retention_loader: Callable[[str], Mapping[str, int]] | None = None,
    ) -> None:
        self._control = control
        self._archive = archive
        self._retention_loader = retention_loader or _configured_domain_retention

    def expire(self, service_id: str, *, limit: int = 100) -> int:
        retention = {
            d: (days + self.MARGIN_DAYS if days > 0 else 0) for d, days in self._retention_loader(service_id).items()
        }
        expired = 0
        for manifest in self._control.expirable_archive_manifests(service_id, retention, limit=limit):
            try:
                self._archive.expire(manifest)
                self._control.mark_archive_expired(manifest.manifest_id)
            except Exception:
                logger.warning(
                    "high-scale archive expiry failed for manifest %s; retrying next sweep",
                    manifest.manifest_id,
                    exc_info=True,
                )
                continue
            expired += 1
        return expired


def _configured_domain_retention(service_id: str) -> dict[str, int]:
    sync_cfg = (config.load_config(service_id) or {}).get("provisioning", {}).get("cron_sync", {})
    data_days = int(sync_cfg.get("data_retention_days", 30))
    rum_days = int(sync_cfg.get("rum_retention_days", data_days))
    return {"request": data_days, "rum_vitals": rum_days, "rum_errors": rum_days}


class HighScaleDeletionSweeper:
    """Periodically deletes raw source objects whose archive is durably verified and whose grace period has elapsed."""

    def __init__(
        self, *, control: Any, controller: DeletionController, archive_expirer: HighScaleArchiveExpirer | None = None
    ) -> None:
        self._control = control
        self._controller = controller
        self._archive_expirer = archive_expirer

    def sweep(self, *, service_id: str, limit: int = 100, now: datetime | None = None) -> DeletionSweepResult:
        result = self._sweep_sources(service_id=service_id, limit=limit, now=now)
        if self._archive_expirer is not None:
            self._archive_expirer.expire(service_id, limit=limit)
        return result

    def _sweep_sources(self, *, service_id: str, limit: int = 100, now: datetime | None = None) -> DeletionSweepResult:
        manifest_to_keys: dict[str, list[str]] = {}
        for source in self._control.deletable_sources(service_id, limit=limit):
            manifest_id = getattr(source, "archive_manifest_id", None)
            if manifest_id:
                manifest_to_keys.setdefault(manifest_id, []).append(source.object_key)
        deleted = skipped = 0
        for manifest_id, keys in manifest_to_keys.items():
            record = self._control.archive_manifest(manifest_id)
            try:
                deleted_keys = self._controller.delete_manifest_sources(
                    record.manifest, current_owner_epoch=record.owner_epoch, source_keys=keys, now=now
                )
                deleted += len(deleted_keys)
                skipped += len(keys) - len(deleted_keys)
            except ValueError:
                skipped += len(keys)
        return DeletionSweepResult(deleted, skipped)
