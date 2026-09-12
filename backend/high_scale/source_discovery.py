"""Bounded, owner-fenced source discovery for the high-scale worker plane."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from backend.high_scale.ledger import HighScaleLedger
from backend.high_scale.ownership import OwnershipStore

MAX_SOURCE_PAGE_SIZE = 500
DEFAULT_SOURCE_PAGE_SIZE = 100
INITIAL_SOURCE_CURSOR = "__initial__"


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


class S3SourceObjectLister:
    """Paginated S3-compatible source listing for the high-scale worker plane."""

    _PREFIXES = {
        "request": "raw/request/",
        "rum_vitals": "raw/rum/",
        "rum_errors": "raw/rum/",
    }

    def __init__(self, client: Any, *, bucket: str, prefix: str = "") -> None:
        if not bucket:
            raise ValueError("bucket is required")
        self._client = client
        self._bucket = bucket
        self._prefix = prefix.strip("/")

    def list_source_objects(
        self,
        service_id: str,
        domain: str,
        *,
        page_size: int,
        cursor: str | None,
    ) -> SourceObjectPage:
        del service_id
        if page_size <= 0 or page_size > MAX_SOURCE_PAGE_SIZE:
            raise ValueError(f"page_size must be between 1 and {MAX_SOURCE_PAGE_SIZE}")
        domain_prefix = self._PREFIXES.get(domain)
        if domain_prefix is None:
            raise ValueError(f"unsupported source domain: {domain}")
        paginator = self._client.get_paginator("list_objects_v2")
        request: dict[str, Any] = {
            "Bucket": self._bucket,
            "Prefix": self._key(domain_prefix),
            "PaginationConfig": {"PageSize": page_size},
        }
        if cursor and cursor != INITIAL_SOURCE_CURSOR:
            request["PaginationConfig"]["StartingToken"] = cursor
        page = next(iter(paginator.paginate(**request)))
        objects = tuple(
            SourceObjectDescriptor(
                object_key=item["Key"],
                checksum=_source_checksum(item),
                size_bytes=int(item.get("Size", 0)),
                version=item.get("VersionId") or item.get("ETag"),
            )
            for item in page.get("Contents", [])
            if isinstance(item.get("Key"), str)
        )
        return SourceObjectPage(objects, page.get("NextToken"))

    def _key(self, key: str) -> str:
        normalized = key.lstrip("/")
        return f"{self._prefix}/{normalized}" if self._prefix else normalized


class S3SourceObjectReader:
    """Reads immutable source objects from an S3-compatible bucket."""

    def __init__(self, client: Any, *, bucket: str) -> None:
        if not bucket:
            raise ValueError("bucket is required")
        self._client = client
        self._bucket = bucket

    def read_source_object(self, service_id: str, domain: str, object_key: str) -> bytes:
        del service_id, domain
        response = self._client.get_object(Bucket=self._bucket, Key=object_key)
        return response["Body"].read()


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


def _source_checksum(item: dict[str, object]) -> str:
    checksum = item.get("ChecksumSHA256") or item.get("ChecksumSHA1")
    if isinstance(checksum, str) and checksum:
        return checksum if ":" in checksum else f"checksum:{checksum}"
    etag = item.get("ETag")
    if isinstance(etag, str) and etag:
        return f"etag:{etag.strip(chr(34))}"
    raise ValueError("source object listing item has no checksum or ETag")
