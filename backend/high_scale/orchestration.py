"""Durable coordination for the isolated high-scale source worker plane.

The coordinator is deliberately not registered with the application scheduler.
Callers own the lifecycle and may provide a Postgres control plane; the small
SQLite stores remain useful for local operation and tests.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from backend.high_scale.ingest_controller import HighScaleIngestController
from backend.high_scale.source_discovery import (
    MAX_SOURCE_PAGE_SIZE,
    TERMINAL_SOURCE_CURSOR_PREFIX,
    SourceObjectDescriptor,
    SourceObjectLister,
)


class SourceObjectReader(Protocol):
    def read_source_object(self, service_id: str, domain: str, object_key: str) -> bytes: ...


class SourceControlPlane(Protocol):
    def owner(self, service_id: str) -> Any: ...

    def discover_source(
        self,
        service_id: str,
        domain: str,
        object_key: str,
        checksum: str,
        *,
        size_bytes: int,
        version: str | None,
        expected_owner: str,
        expected_owner_epoch: int,
    ) -> Any: ...

    def claim_source(
        self,
        service_id: str,
        object_key: str,
        worker_id: str,
        *,
        expected_owner: str,
        expected_owner_epoch: int,
        lease_seconds: float,
        now: datetime | None = None,
    ) -> Any: ...


@dataclass(frozen=True)
class PageRun:
    cursor: str | None
    next_cursor: str | None
    discovered: int
    processed: int
    duplicates: int
    failed: int


@dataclass(frozen=True)
class SweepResult:
    retried: int
    replayed: int
    skipped: int


def _owner_fields(owner: Any) -> tuple[str, int, str | None]:
    return owner.current_owner, int(owner.owner_epoch), getattr(owner, "source_cursor", None)


class HighScaleWorkerCoordinator:
    """Runs bounded discovery and processing under an owner-epoch fence."""

    def __init__(
        self,
        *,
        lister: SourceObjectLister,
        reader: SourceObjectReader,
        controller: HighScaleIngestController,
        ownership: Any,
        ledger: Any,
        worker_id: str,
        control_plane: SourceControlPlane | None = None,
        page_size: int = 100,
        lease_seconds: float = 300.0,
    ) -> None:
        if not worker_id:
            raise ValueError("worker identity is required")
        if page_size <= 0 or page_size > MAX_SOURCE_PAGE_SIZE:
            raise ValueError(f"page_size must be between 1 and {MAX_SOURCE_PAGE_SIZE}")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        self._lister = lister
        self._reader = reader
        self._controller = controller
        self._ownership = ownership
        self._ledger = ledger
        self._worker_id = worker_id
        self._control = control_plane
        self._page_size = page_size
        self._lease_seconds = lease_seconds

    def run_page(
        self,
        *,
        service_id: str,
        domain: str,
        cursor: str | None = None,
        now: datetime | None = None,
    ) -> PageRun:
        owner = self._owner(service_id)
        current_owner, epoch, persisted_cursor = _owner_fields(owner)
        self._require_high_scale(current_owner, service_id)
        listing_cursor = persisted_cursor if cursor is None else cursor
        page = self._lister.list_source_objects(
            service_id,
            domain,
            page_size=self._page_size,
            cursor=listing_cursor,
        )
        objects = tuple(page.objects)
        if len(objects) > self._page_size:
            raise ValueError("source object listing exceeded page_size")

        for source in objects:
            self._discover(service_id, domain, source, current_owner, epoch)
        processed = duplicates = failed = 0
        for source in objects:
            outcome = self._process_source(
                service_id,
                domain,
                source,
                expected_owner=current_owner,
                expected_epoch=epoch,
                now=now,
            )
            if outcome == "processed":
                processed += 1
            elif outcome == "duplicate":
                duplicates += 1
            else:
                failed += 1
        next_cursor = page.next_cursor
        if next_cursor is None:
            if objects:
                next_cursor = f"{TERMINAL_SOURCE_CURSOR_PREFIX}{objects[-1].object_key}"
            elif listing_cursor and listing_cursor.startswith(TERMINAL_SOURCE_CURSOR_PREFIX):
                next_cursor = listing_cursor
        if failed == 0:
            self._advance_cursor(service_id, next_cursor, current_owner, epoch)
        return PageRun(listing_cursor, next_cursor, len(objects), processed, duplicates, failed)

    def process_source(
        self,
        *,
        service_id: str,
        domain: str,
        source: SourceObjectDescriptor,
        now: datetime | None = None,
    ) -> str:
        owner = self._owner(service_id)
        current_owner, epoch, _ = _owner_fields(owner)
        self._require_high_scale(current_owner, service_id)
        return self._process_source(
            service_id,
            domain,
            source,
            expected_owner=current_owner,
            expected_epoch=epoch,
            now=now,
        )

    def _process_source(
        self,
        service_id: str,
        domain: str,
        source: SourceObjectDescriptor,
        *,
        expected_owner: str,
        expected_epoch: int,
        now: datetime | None,
    ) -> str:
        if self._already_terminal(service_id, source.object_key):
            return "duplicate"
        try:
            if self._control is not None:
                claim = self._control.claim_source(
                    service_id,
                    source.object_key,
                    self._worker_id,
                    expected_owner=expected_owner,
                    expected_owner_epoch=expected_epoch,
                    lease_seconds=self._lease_seconds,
                    now=now,
                )
                if not claim.claimed:
                    return "failed"
            self._controller.ingest(
                service_id=service_id,
                domain=domain,
                object_key=source.object_key,
                checksum=source.checksum,
                payload=self._reader.read_source_object(service_id, domain, source.object_key),
                size_bytes=source.size_bytes,
                version=source.version,
                now=now,
            )
        except Exception:
            return "failed"
        return "processed"

    def _owner(self, service_id: str) -> Any:
        return self._control.owner(service_id) if self._control is not None else self._ownership.get(service_id)

    def _discover(
        self,
        service_id: str,
        domain: str,
        source: SourceObjectDescriptor,
        expected_owner: str,
        epoch: int,
    ) -> None:
        if self._control is not None:
            self._control.discover_source(
                service_id,
                domain,
                source.object_key,
                source.checksum,
                size_bytes=source.size_bytes,
                version=source.version,
                expected_owner=expected_owner,
                expected_owner_epoch=epoch,
            )
        else:
            self._ledger.discover(
                service_id,
                domain,
                source.object_key,
                source.checksum,
                size_bytes=source.size_bytes,
                version=source.version,
            )

    def _advance_cursor(self, service_id: str, cursor: str | None, owner: str, epoch: int) -> None:
        if cursor is None:
            return
        if self._control is not None and hasattr(self._control, "advance_source_cursor"):
            self._control.advance_source_cursor(
                service_id,
                cursor,
                expected_owner=owner,
                expected_owner_epoch=epoch,
            )
        elif hasattr(self._ownership, "advance_cursor"):
            self._ownership.advance_cursor(
                service_id,
                cursor,
                expected_owner=owner,
                expected_epoch=epoch,
            )

    def _already_terminal(self, service_id: str, object_key: str) -> bool:
        source = None
        if self._control is not None and hasattr(self._control, "source"):
            try:
                source = self._control.source(service_id, object_key)
            except KeyError:
                source = None
        elif hasattr(self._ledger, "source"):
            source = self._ledger.source(service_id, object_key)
        elif hasattr(self._ledger, "get_source"):
            source = self._ledger.get_source(service_id, object_key)
        return getattr(source, "status", None) in {"appended", "archived", "acknowledged", "source_deleted"}

    @staticmethod
    def _require_high_scale(owner: str, service_id: str) -> None:
        if owner != "high_scale":
            raise RuntimeError(f"high-scale worker is not the owner for {service_id}")


class HighScaleLeaseSweeper:
    """Retries expired work and replays archived work after lost acknowledgements."""

    def __init__(
        self,
        *,
        ownership: Any,
        ledger: Any,
        worker: HighScaleWorkerCoordinator,
        replay: Callable[[Any], Any] | None = None,
        replay_leases: Any | None = None,
    ) -> None:
        self._ownership = ownership
        self._ledger = ledger
        self._worker = worker
        self._replay = replay
        self._replay_leases = replay_leases

    def sweep(
        self,
        *,
        service_id: str,
        domain: str,
        objects: Iterable[SourceObjectDescriptor],
        now: datetime | None = None,
    ) -> SweepResult:
        owner = self._ownership.get(service_id)
        self._worker._require_high_scale(owner.current_owner, service_id)
        retried = replayed = skipped = 0
        for source in objects:
            record = self._ledger.source(service_id, source.object_key) if hasattr(self._ledger, "source") else None
            status = getattr(record, "status", None)
            if status in {"acknowledged", "source_deleted"}:
                skipped += 1
                continue
            manifest_id = getattr(record, "archive_manifest_id", None)
            if status == "archived" and manifest_id and self._replay is not None:
                if self._replay_leases is not None and self._replay_leases.active(manifest_id):
                    skipped += 1
                    continue
                self._replay(manifest_id)
                if hasattr(self._ledger, "acknowledge"):
                    self._ledger.acknowledge(service_id, source.object_key, manifest_id)
                replayed += 1
                continue
            try:
                outcome = self._worker.process_source(service_id=service_id, domain=domain, source=source, now=now)
                if outcome == "processed":
                    retried += 1
                else:
                    skipped += 1
            except (KeyError, RuntimeError):
                skipped += 1
        return SweepResult(retried, replayed, skipped)


HighScaleWorker = HighScaleWorkerCoordinator
HighScaleSweeper = HighScaleLeaseSweeper
