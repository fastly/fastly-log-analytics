"""Bounded, owner-fenced source discovery for the high-scale worker plane."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

from backend.high_scale.ledger import HighScaleLedger
from backend.high_scale.ownership import OwnershipStore

MAX_SOURCE_PAGE_SIZE = 500
DEFAULT_SOURCE_PAGE_SIZE = 100


@dataclass(frozen=True)
class SourceObjectDescriptor:
    object_key: str
    checksum: str
    size_bytes: int = 0
    version: str | None = None


@dataclass(frozen=True)
class SourceObjectPage:
    objects: Sequence[SourceObjectDescriptor]
    next_cursor: str | None


class SourceObjectLister(Protocol):
    def list_source_objects(
        self,
        service_id: str,
        domain: str,
        *,
        page_size: int,
        cursor: str | None,
    ) -> SourceObjectPage: ...


@dataclass(frozen=True)
class DiscoveryResult:
    next_cursor: str | None
    discovered_count: int


class HighScaleSourceDiscovery:
    """Discovers one bounded page while enforcing high-scale ownership."""

    def __init__(
        self,
        *,
        ownership: OwnershipStore,
        ledger: HighScaleLedger,
        lister: SourceObjectLister,
        page_size: int = DEFAULT_SOURCE_PAGE_SIZE,
        owner_epoch: int | None = None,
    ) -> None:
        if page_size <= 0 or page_size > MAX_SOURCE_PAGE_SIZE:
            raise ValueError(f"page_size must be between 1 and {MAX_SOURCE_PAGE_SIZE}")
        if owner_epoch is not None and owner_epoch <= 0:
            raise ValueError("owner_epoch must be positive")
        self._ownership = ownership
        self._ledger = ledger
        self._lister = lister
        self._page_size = page_size
        self._owner_epoch = owner_epoch

    def discover_once(
        self,
        *,
        service_id: str,
        domain: str,
        cursor: str | None = None,
    ) -> DiscoveryResult:
        owner = self._ownership.get(service_id)
        if owner.current_owner != "high_scale":
            raise RuntimeError(f"high-scale discovery is not the owner for {service_id}")
        if owner.owner_epoch <= 0 or (self._owner_epoch is not None and owner.owner_epoch != self._owner_epoch):
            raise RuntimeError(f"high-scale owner epoch does not match for {service_id}")

        listing_cursor = owner.source_cursor if cursor is None else cursor
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
            self._ledger.discover(
                service_id,
                domain,
                source.object_key,
                source.checksum,
                size_bytes=source.size_bytes,
                version=source.version,
            )
        return DiscoveryResult(page.next_cursor, len(objects))
