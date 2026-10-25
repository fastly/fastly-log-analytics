"""Bounded, owner-fenced source discovery for the high-scale worker plane."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Protocol


class SourceObjectMissingError(RuntimeError):
    """Raised when a claimed source object is confirmed gone from storage.

    Distinct from a transient read failure: this means the raw object was
    deleted (e.g. by a legacy raw-deletion path racing high-scale ownership)
    and cannot be recovered. Callers must record this as a durable failure
    rather than silently retrying forever or treating it as processed.
    """


MAX_SOURCE_PAGE_SIZE = 500
INITIAL_SOURCE_CURSOR = "__initial__"
TERMINAL_SOURCE_CURSOR_PREFIX = "__terminal__:"
RUM_REINSPECTION_INTERVAL = 10

RUM_SOURCE_DOMAINS = frozenset({"rum_vitals", "rum_errors"})


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
    next_reinspection_cursor: str | None = None
    forward_pages_since_reinspection: int | None = None
    reinspection_page: bool = False


class SourceObjectLister(Protocol):
    def list_source_objects(
        self,
        service_id: str,
        domain: str,
        *,
        page_size: int,
        cursor: str | None,
        reinspection_cursor: str | None = None,
        reinspect: bool = False,
        forward_pages_since_reinspection: int = 0,
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
        reinspection_cursor: str | None = None,
        reinspect: bool = False,
        forward_pages_since_reinspection: int = 0,
    ) -> SourceObjectPage:
        del service_id
        if page_size <= 0 or page_size > MAX_SOURCE_PAGE_SIZE:
            raise ValueError(f"page_size must be between 1 and {MAX_SOURCE_PAGE_SIZE}")
        domain_prefix = self._PREFIXES.get(domain)
        if domain_prefix is None:
            raise ValueError(f"unsupported source domain: {domain}")
        resolved_prefix = self._key(domain_prefix)
        if reinspect and domain not in RUM_SOURCE_DOMAINS:
            raise ValueError("only RUM source domains support reinspection")
        if forward_pages_since_reinspection < 0:
            raise ValueError("forward_pages_since_reinspection cannot be negative")
        listing_cursor = (reinspection_cursor or INITIAL_SOURCE_CURSOR) if reinspect else cursor
        paginator = self._client.get_paginator("list_objects_v2")
        request: dict[str, Any] = {
            "Bucket": self._bucket,
            "Prefix": resolved_prefix,
            "PaginationConfig": {"PageSize": page_size},
        }
        if listing_cursor and listing_cursor != INITIAL_SOURCE_CURSOR:
            if listing_cursor.startswith(TERMINAL_SOURCE_CURSOR_PREFIX):
                start_after = listing_cursor.removeprefix(TERMINAL_SOURCE_CURSOR_PREFIX)
                # Defense in depth against a foreign-domain cursor slipping
                # through (e.g. from the owner-level fallback this class of
                # bug came from): a StartAfter key outside this domain's own
                # Prefix isn't a valid resume point and can silently return
                # zero objects forever instead of erroring — treat it as no
                # cursor rather than starving discovery.
                if start_after.startswith(resolved_prefix):
                    request["StartAfter"] = start_after
            else:
                request["PaginationConfig"]["StartingToken"] = listing_cursor
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
        next_cursor = page.get("NextToken")
        if domain not in RUM_SOURCE_DOMAINS:
            return SourceObjectPage(objects, next_cursor)
        if reinspect:
            # This page is from the independent cursor. Keep the main cursor
            # byte-for-byte unchanged for older worker versions and advance
            # only the separate persisted reinspection position. S3 returns
            # NextContinuationToken, not boto's paginator StartingToken; using
            # the last key with StartAfter is the bounded, resumable cursor.
            has_more = bool(page.get("NextContinuationToken"))
            next_reinspection_cursor = (
                f"{TERMINAL_SOURCE_CURSOR_PREFIX}{objects[-1].object_key}"
                if has_more and objects
                else INITIAL_SOURCE_CURSOR
            )
            return SourceObjectPage(
                objects,
                cursor or INITIAL_SOURCE_CURSOR,
                next_reinspection_cursor=next_reinspection_cursor,
                forward_pages_since_reinspection=0,
                reinspection_page=True,
            )
        return SourceObjectPage(
            objects,
            next_cursor,
            next_reinspection_cursor=reinspection_cursor or INITIAL_SOURCE_CURSOR,
            forward_pages_since_reinspection=forward_pages_since_reinspection + 1,
        )

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
        try:
            response = self._client.get_object(Bucket=self._bucket, Key=object_key)
        except Exception as exc:
            error_code = _client_error_code(exc)
            if error_code in {"NoSuchKey", "404"}:
                raise SourceObjectMissingError(f"source object is missing from storage: {object_key}") from exc
            raise
        return response["Body"].read()


def _client_error_code(exc: Exception) -> str | None:
    """Extract the S3/boto3 error code without importing botocore directly.

    Keeps this module usable against any S3-compatible client/test double
    that raises a plain exception carrying a ``response`` dict shaped like
    botocore's ``ClientError`` (``exc.response["Error"]["Code"]``).
    """
    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return None
    error = response.get("Error")
    if not isinstance(error, dict):
        return None
    code = error.get("Code")
    return code if isinstance(code, str) else None


def _source_checksum(item: dict[str, object]) -> str:
    checksum = item.get("ChecksumSHA256") or item.get("ChecksumSHA1")
    if isinstance(checksum, str) and checksum:
        return checksum if ":" in checksum else f"checksum:{checksum}"
    etag = item.get("ETag")
    if isinstance(etag, str) and etag:
        return f"etag:{etag.strip(chr(34))}"
    raise ValueError("source object listing item has no checksum or ETag")
