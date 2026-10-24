"""Durable coordination for the isolated high-scale source worker plane.

The coordinator is deliberately not registered with the application scheduler.
Callers own the lifecycle and may provide a Postgres control plane; the small
SQLite stores remain useful for local operation and tests.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from backend.high_scale.ingest_controller import HighScaleIngestController
from backend.high_scale.source_discovery import (
    MAX_SOURCE_PAGE_SIZE,
    TERMINAL_SOURCE_CURSOR_PREFIX,
    SourceObjectDescriptor,
    SourceObjectLister,
    SourceObjectMissingError,
)

logger = logging.getLogger(__name__)


class SourceObjectReader(Protocol):
    def read_source_object(self, service_id: str, domain: str, object_key: str) -> bytes: ...


class SourceControlPlane(Protocol):
    def owner(self, service_id: str) -> Any: ...

    def source_cursor_for(self, service_id: str, domain: str) -> str: ...

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
    # Source objects confirmed permanently gone from storage (recorded as a
    # durable ``source_missing`` failure, not silently retried). Unlike
    # ``failed``, these do NOT block cursor advancement — the data is
    # unrecoverable regardless of retries, so blocking every later object in
    # the stream behind one is strictly worse than recording and moving on.
    missing: int = 0


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
        object_concurrency: int = 1,
    ) -> None:
        if not worker_id:
            raise ValueError("worker identity is required")
        if page_size <= 0 or page_size > MAX_SOURCE_PAGE_SIZE:
            raise ValueError(f"page_size must be between 1 and {MAX_SOURCE_PAGE_SIZE}")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        if object_concurrency <= 0:
            raise ValueError("object_concurrency must be positive")
        self._lister = lister
        self._reader = reader
        self._controller = controller
        self._ownership = ownership
        self._ledger = ledger
        self._worker_id = worker_id
        self._control = control_plane
        self._page_size = page_size
        self._lease_seconds = lease_seconds
        self._object_concurrency = object_concurrency

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
        if cursor is None:
            cursor_reader = self._control if self._control is not None else self._ownership
            if hasattr(cursor_reader, "source_cursor_for"):
                persisted_cursor = cursor_reader.source_cursor_for(service_id, domain)
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

        expired_sources = self._expired_claim_descriptors(service_id, domain)
        discovered_by_key: dict[str, Any] = {
            source.object_key: self._discover(service_id, domain, source, current_owner, epoch) for source in objects
        }

        next_cursor = page.next_cursor
        if next_cursor is None:
            if objects:
                next_cursor = f"{TERMINAL_SOURCE_CURSOR_PREFIX}{objects[-1].object_key}"
            elif listing_cursor and listing_cursor.startswith(TERMINAL_SOURCE_CURSOR_PREFIX):
                next_cursor = listing_cursor

        processed = 0
        duplicates = 0
        failed = 0
        missing = 0

        active_sources: list[SourceObjectDescriptor] = list(expired_sources)
        active_keys = {s.object_key for s in active_sources}
        for s in objects:
            if s.object_key in active_keys:
                continue
            rec = discovered_by_key.get(s.object_key)
            rec_status = getattr(rec, "status", None)
            if isinstance(rec_status, str):
                is_terminal = rec_status in _TERMINAL_SOURCE_STATES
            else:
                is_terminal = self._already_terminal(service_id, s.object_key)
            if is_terminal:
                duplicates += 1
            else:
                active_sources.append(s)
                active_keys.add(s.object_key)

        if active_sources:
            unclaimable_keys: set[str] = set()
            if self._control is not None and (
                getattr(type(self._control), "claim_sources_batch", None) is not None
                or "claim_sources_batch" in vars(self._control)
                or "claim_sources_batch" in getattr(self._control, "_mock_children", {})
            ):
                keys = tuple(s.object_key for s in active_sources)
                claim_batch_fn = getattr(self._control, "claim_sources_batch")
                claims = claim_batch_fn(
                    service_id,
                    keys,
                    self._worker_id,
                    expected_owner=current_owner,
                    expected_owner_epoch=epoch,
                    lease_seconds=self._lease_seconds,
                    now=now,
                )
                claimed_keys = {c.object_key for c in claims}
            elif self._control is not None:
                claimed_keys = set()
                for s in active_sources:
                    try:
                        c = self._control.claim_source(
                            service_id,
                            s.object_key,
                            self._worker_id,
                            expected_owner=current_owner,
                            expected_owner_epoch=epoch,
                            lease_seconds=self._lease_seconds,
                            now=now,
                        )
                        if c.claimed:
                            claimed_keys.add(s.object_key)
                    except ValueError:
                        if not self._already_terminal(service_id, s.object_key):
                            failed += 1
                            unclaimable_keys.add(s.object_key)
            else:
                claimed_keys = set()
                for s in active_sources:
                    c = self._ledger.claim(service_id, s.object_key, self._worker_id)
                    if c.claimed:
                        claimed_keys.add(s.object_key)

            to_read: list[SourceObjectDescriptor] = []
            for s in active_sources:
                if s.object_key in claimed_keys:
                    to_read.append(s)
                elif s.object_key not in unclaimable_keys:
                    duplicates += 1

            if to_read:

                def read_one(
                    src: SourceObjectDescriptor,
                ) -> tuple[SourceObjectDescriptor, bytes | None, str | None, bool]:
                    try:
                        data = self._reader.read_source_object(service_id, domain, src.object_key)
                        return (src, data, None, False)
                    except SourceObjectMissingError as exc:
                        return (src, None, str(exc), True)
                    except Exception as exc:
                        return (src, None, str(exc), False)

                workers = min(self._object_concurrency, len(to_read))
                if workers <= 1:
                    read_results = [read_one(s) for s in to_read]
                else:
                    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="high-scale-batch-reader") as pool:
                        read_results = list(pool.map(read_one, to_read))

                valid_payloads: list[tuple[SourceObjectDescriptor, bytes]] = []
                for src, data, err, is_missing in read_results:
                    if is_missing:
                        self._mark_missing(
                            service_id,
                            object_key=src.object_key,
                            expected_owner=current_owner,
                            expected_epoch=epoch,
                            error=err or "missing",
                            now=now,
                        )
                        missing += 1
                    elif err is not None:
                        logger.exception(
                            "high-scale source read failed",
                            extra={
                                "service_id": service_id,
                                "domain": domain,
                                "object_key": src.object_key,
                                "error": err,
                            },
                        )
                        failed += 1
                    elif data is not None:
                        valid_payloads.append((src, data))

                if valid_payloads:
                    try:
                        batch_next_cursor = None if failed > 0 else next_cursor
                        batch_res = self._controller.ingest_batch(
                            service_id=service_id,
                            domain=domain,
                            sources_payloads=valid_payloads,
                            next_cursor=batch_next_cursor,
                            now=now,
                        )
                        processed += batch_res.sources_count
                    except Exception as exc:
                        logger.exception(
                            "high-scale batch ingest failed",
                            extra={"service_id": service_id, "domain": domain, "count": len(valid_payloads)},
                        )
                        if self._control is None and hasattr(self._ledger, "mark_failed"):
                            for src, _ in valid_payloads:
                                cur = self._ledger.source(service_id, src.object_key)
                                if cur is not None:
                                    self._ledger.mark_failed(service_id, src.object_key, cur.lease_generation, str(exc))
                        failed += len(valid_payloads)

        if failed == 0:
            self._advance_cursor(service_id, domain, next_cursor, current_owner, epoch)
        return PageRun(listing_cursor, next_cursor, len(objects), processed, duplicates, failed, missing)

    def _expired_claim_descriptors(
        self,
        service_id: str,
        domain: str,
    ) -> list[SourceObjectDescriptor]:
        source_store = self._control if self._control is not None else self._ledger
        expired_claims = getattr(source_store, "expired_claims", None)
        if expired_claims is None:
            return []
        return [
            SourceObjectDescriptor(
                object_key=source.object_key,
                checksum=source.checksum,
                size_bytes=source.size_bytes,
                version=source.version,
            )
            for source in expired_claims(service_id, domain, limit=self._page_size)
        ]

    def _mark_missing(
        self,
        service_id: str,
        *,
        object_key: str,
        expected_owner: str,
        expected_epoch: int,
        error: str,
        now: datetime | None,
    ) -> None:
        if self._control is not None and hasattr(self._control, "mark_source_missing"):
            self._control.mark_source_missing(
                service_id,
                object_key,
                expected_owner=expected_owner,
                expected_owner_epoch=expected_epoch,
                error=error,
                now=now,
            )
        elif hasattr(self._ledger, "mark_source_missing"):
            self._ledger.mark_source_missing(service_id, object_key, error=error)

    def _owner(self, service_id: str) -> Any:
        return self._control.owner(service_id) if self._control is not None else self._ownership.get(service_id)

    def _discover(
        self,
        service_id: str,
        domain: str,
        source: SourceObjectDescriptor,
        expected_owner: str,
        epoch: int,
    ) -> Any:
        if self._control is not None:
            return self._control.discover_source(
                service_id,
                domain,
                source.object_key,
                source.checksum,
                size_bytes=source.size_bytes,
                version=source.version,
                expected_owner=expected_owner,
                expected_owner_epoch=epoch,
            )
        return self._ledger.discover(
            service_id,
            domain,
            source.object_key,
            source.checksum,
            size_bytes=source.size_bytes,
            version=source.version,
        )

    def _advance_cursor(self, service_id: str, domain: str, cursor: str | None, owner: str, epoch: int) -> None:
        if cursor is None:
            return
        if self._control is not None and hasattr(self._control, "advance_source_cursor"):
            self._control.advance_source_cursor(
                service_id,
                cursor,
                expected_owner=owner,
                expected_owner_epoch=epoch,
                domain=domain,
            )
        elif hasattr(self._ownership, "advance_cursor"):
            self._ownership.advance_cursor(
                service_id,
                cursor,
                expected_owner=owner,
                expected_epoch=epoch,
                domain=domain,
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
        return getattr(source, "status", None) in _TERMINAL_SOURCE_STATES

    @staticmethod
    def _require_high_scale(owner: str, service_id: str) -> None:
        if owner != "high_scale":
            raise RuntimeError(f"high-scale worker is not the owner for {service_id}")


_TERMINAL_SOURCE_STATES = frozenset(
    {
        "appended",
        "archived",
        "acknowledged",
        "source_deleted",
        "source_missing",
    }
)
