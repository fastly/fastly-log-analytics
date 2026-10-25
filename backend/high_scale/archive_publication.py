"""Verified archive publication against an injected object store."""

from __future__ import annotations

import hashlib
import json
import sys
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.parquet as pq

from backend.high_scale.archive_models import (
    ArchiveArtifact,
    ArchiveManifest,
    ArchiveSourceEntry,
    ArchiveSourceObject,
)
from backend.utils.structlog_config import atomic_temp_path, atomic_write_json


class ObjectStore(Protocol):
    def put(self, key: str, payload: bytes) -> None: ...

    def get(self, key: str) -> bytes: ...

    def exists(self, key: str) -> bool: ...

    def delete(self, key: str) -> None: ...


class PublicationState(StrEnum):
    ARTIFACT_VERIFIED = "artifact_verified"
    MANIFEST_PREPARED = "manifest_prepared"
    MANIFEST_COMMITTED = "manifest_committed"


@dataclass(frozen=True)
class PublicationResult:
    manifest_id: str
    state: PublicationState
    artifact_key: str
    manifest_key: str
    commit_key: str


class InMemoryObjectStore:
    """Small deterministic store used by contract tests and local replay tests."""

    def __init__(self) -> None:
        self._objects: dict[str, bytes] = {}

    def put(self, key: str, payload: bytes) -> None:
        self._objects[key] = bytes(payload)

    def get(self, key: str) -> bytes:
        return self._objects[key]

    def exists(self, key: str) -> bool:
        return key in self._objects

    def delete(self, key: str) -> None:
        self._objects.pop(key, None)


class S3ObjectStore:
    """S3-compatible object store adapter for FOS and local S3 emulators."""

    def __init__(self, client: Any, *, bucket: str, prefix: str = "") -> None:
        if not bucket:
            raise ValueError("bucket is required")
        self._client = client
        self._bucket = bucket
        self._prefix = prefix.strip("/")

    def put(self, key: str, payload: bytes) -> None:
        self._client.put_object(Bucket=self._bucket, Key=self._key(key), Body=payload)

    def get(self, key: str) -> bytes:
        response = self._client.get_object(Bucket=self._bucket, Key=self._key(key))
        return response["Body"].read()

    def exists(self, key: str) -> bool:
        try:
            self._client.head_object(Bucket=self._bucket, Key=self._key(key))
        except Exception as exc:
            if _is_not_found(exc):
                return False
            raise
        return True

    def delete(self, key: str) -> None:
        self._client.delete_object(Bucket=self._bucket, Key=self._key(key))

    def _key(self, key: str) -> str:
        normalized = key.strip("/")
        return f"{self._prefix}/{normalized}" if self._prefix else normalized


class ArchivePublication:
    def __init__(self, store: ObjectStore) -> None:
        self._store = store

    def publish(self, manifest: ArchiveManifest, artifact: bytes) -> PublicationResult:
        manifest.validate()
        artifact_key = _artifact_key(manifest.artifact.uri)
        manifest_key = f"archive/manifests/{manifest.manifest_id}.json"
        commit_key = f"archive/manifests/{manifest.manifest_id}.commit"
        if _checksum(artifact) != manifest.artifact.checksum:
            raise ValueError("artifact checksum does not match manifest")
        if len(artifact) != manifest.artifact.size_bytes:
            raise ValueError("artifact size does not match manifest")

        # Archive manifests are immutable, content-addressed artifacts keyed
        # by manifest_id — a retry of the same logical publish (e.g. after a
        # crash between this call and the caller's next step) must be a
        # no-op, not a second write. manifest_id doesn't depend on
        # coverage_start/coverage_end, so a retry computed from a fresh
        # `now` produces a byte-different manifest payload for the SAME
        # key; overwriting an already-published key with different content
        # both breaks the immutability contract and, against a CDN-cached
        # store, can make the immediate post-write verification read back
        # stale bytes and fail spuriously (observed live during
        # qualification — every retry of one object failed here forever).
        if self.is_replayable(manifest.manifest_id):
            return PublicationResult(
                manifest.manifest_id,
                PublicationState.MANIFEST_COMMITTED,
                artifact_key,
                manifest_key,
                commit_key,
            )

        self._store.put(artifact_key, artifact)
        if self._store.get(artifact_key) != artifact:
            raise ValueError("artifact verification failed after upload")

        manifest_payload = _json_bytes(asdict(manifest))
        self._store.put(manifest_key, manifest_payload)
        if self._store.get(manifest_key) != manifest_payload:
            raise ValueError("manifest verification failed after upload")

        commit_payload = _json_bytes(
            {
                "manifest_id": manifest.manifest_id,
                "manifest_checksum": _checksum(manifest_payload),
                "artifact_checksum": manifest.artifact.checksum,
            }
        )
        self._store.put(commit_key, commit_payload)
        if not self.is_replayable(manifest.manifest_id):
            raise ValueError("manifest commit marker verification failed")
        return PublicationResult(
            manifest.manifest_id,
            PublicationState.MANIFEST_COMMITTED,
            artifact_key,
            manifest_key,
            commit_key,
        )

    def state(self, manifest_id: str) -> PublicationState | None:
        commit_key = f"archive/manifests/{manifest_id}.commit"
        if not self._store.exists(commit_key):
            return None
        if self.is_replayable(manifest_id):
            return PublicationState.MANIFEST_COMMITTED
        return None

    def _read_verified_artifact(self, manifest_id: str) -> bytes | None:
        manifest_key = f"archive/manifests/{manifest_id}.json"
        commit_key = f"archive/manifests/{manifest_id}.commit"
        if not self._store.exists(manifest_key) or not self._store.exists(commit_key):
            return None
        try:
            manifest_payload = self._store.get(manifest_key)
            commit = json.loads(self._store.get(commit_key))
            manifest = _manifest_from_json(manifest_payload)
            manifest.validate()
            if commit.get("manifest_id") != manifest_id:
                return None
            if commit.get("manifest_checksum") != _checksum(manifest_payload):
                return None
            if commit.get("artifact_checksum") != manifest.artifact.checksum:
                return None
            artifact = self._store.get(_artifact_key(manifest.artifact.uri))
            if _checksum(artifact) == manifest.artifact.checksum and len(artifact) == manifest.artifact.size_bytes:
                return artifact
            return None
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            return None

    def is_replayable(self, manifest_id: str) -> bool:
        return self._read_verified_artifact(manifest_id) is not None

    def read_artifact(self, manifest: ArchiveManifest) -> bytes:
        artifact = self._read_verified_artifact(manifest.manifest_id)
        if artifact is None:
            raise ValueError("archive manifest is not replayable")
        return artifact


def _artifact_key(uri: str) -> str:
    parsed = urlparse(uri)
    if parsed.scheme in {"s3", "https"}:
        key = parsed.path.lstrip("/")
        if parsed.scheme == "s3" and parsed.netloc:
            return f"{parsed.netloc}/{key}" if key else parsed.netloc
        return key
    return uri.removeprefix("file://").lstrip("/")


def _manifest_from_json(payload: bytes) -> ArchiveManifest:
    raw = json.loads(payload)
    raw_sources = raw.get("sources")
    sources = (
        [
            ArchiveSourceEntry(
                key=s["key"],
                checksum=s["checksum"],
                size_bytes=s["size_bytes"],
                row_start=s["row_start"],
                row_count=s["row_count"],
                deletion_deadline=_parse_datetime(s["deletion_deadline"]),
                version=s.get("version"),
            )
            for s in raw_sources
        ]
        if raw_sources
        else None
    )
    return ArchiveManifest(
        manifest_id=raw["manifest_id"],
        source=ArchiveSourceObject(**raw["source"]) if raw.get("source") else None,
        artifact=ArchiveArtifact(**raw["artifact"]),
        coverage_start=_parse_datetime(raw["coverage_start"]),
        coverage_end=_parse_datetime(raw["coverage_end"]),
        retention_deadline=_parse_datetime(raw["retention_deadline"]),
        deletion_authorization_deadline=_parse_datetime(raw["deletion_authorization_deadline"]),
        archive_epoch=raw["archive_epoch"],
        service_id=raw.get("service_id"),
        domain=raw.get("domain"),
        sources=sources,
    )


def _parse_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _json_bytes(value: dict[str, Any]) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=_json_default).encode()


def _checksum(value: bytes) -> str:
    return f"sha256:{hashlib.sha256(value).hexdigest()}"


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"cannot serialize {type(value).__name__}")


def _is_not_found(exc: Exception) -> bool:
    response = getattr(exc, "response", None)
    return isinstance(response, dict) and response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def event_digest(events: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> str:
    keys = set().union(*(event.keys() for event in events))
    digest = hashlib.sha256()
    for event in events:
        canonical = {key: _parquet_safe_value(event.get(key)) for key in keys}
        digest.update(
            (json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode()
        )
    return f"sha256:{digest.hexdigest()}"


def write_batch_archive_checkpoint(
    root: str | Path,
    *,
    service_id: str,
    domain: str,
    source_batches: list[tuple[ArchiveSourceObject, list[dict[str, Any]]]]
    | tuple[tuple[ArchiveSourceObject, list[dict[str, Any]]], ...],
    archive_epoch: int,
    schema_version: str,
    transform_version: str,
    coverage_start: datetime,
    coverage_end: datetime,
    retention_seconds: int = 0,
    deletion_grace_seconds: int = 900,
) -> ArchiveManifest:
    if not source_batches:
        raise ValueError("archive checkpoint cannot be empty")
    for source, _ in source_batches:
        if source.service_id != service_id or source.domain != domain:
            raise ValueError("source identity does not match archive batch")
    if coverage_end < coverage_start:
        raise ValueError("archive coverage end precedes coverage start")
    if archive_epoch < 0:
        raise ValueError("archive epoch must be non-negative")
    if retention_seconds < 0 or deletion_grace_seconds < 0:
        raise ValueError("retention and deletion grace periods must be non-negative")

    total_events = sum(len(events) for _, events in source_batches)
    if total_events == 0:
        raise ValueError("archive checkpoint cannot be empty")

    output_root = Path(root)
    output_root.mkdir(parents=True, exist_ok=True)

    all_events: list[dict[str, Any]] = []
    source_entries: list[ArchiveSourceEntry] = []
    current_offset = 0

    deletion_deadline = coverage_end.astimezone(UTC) + timedelta(seconds=retention_seconds + deletion_grace_seconds)
    for source, events in source_batches:
        row_count = len(events)
        source_entries.append(
            ArchiveSourceEntry(
                key=source.object_key,
                checksum=source.checksum,
                size_bytes=source.size_bytes,
                row_start=current_offset,
                row_count=row_count,
                deletion_deadline=deletion_deadline,
                version=source.version,
            )
        )
        current_offset += row_count
        all_events.extend(events)

    keys = set().union(*(event.keys() for event in all_events))
    canonical_events = [{key: _parquet_safe_value(event.get(key)) for key in keys} for event in all_events]
    artifact_digest = event_digest(canonical_events)

    if len(source_batches) == 1:
        single_source = source_batches[0][0]
        source_token = hashlib.sha256(f"{single_source.object_key}:{single_source.checksum}".encode()).hexdigest()[:16]
    else:
        source_token = hashlib.sha256(
            ":".join(f"{s.object_key}:{s.checksum}" for s, _ in source_batches).encode()
        ).hexdigest()[:16]

    artifact_name = f"{service_id}-{domain}-{source_token}.parquet"
    artifact_path = output_root / artifact_name
    with atomic_temp_path(artifact_path, suffix=".parquet") as temp_path:
        pq.write_table(pa.Table.from_pylist(canonical_events), temp_path)

    artifact_checksum = _sha256(artifact_path)
    artifact = ArchiveArtifact(
        uri=artifact_path.as_uri(),
        checksum=artifact_checksum,
        size_bytes=artifact_path.stat().st_size,
        row_count=len(all_events),
        byte_count=sum(
            len(json.dumps(event, separators=(",", ":"), ensure_ascii=False).encode()) for event in canonical_events
        ),
        canonical_digest=artifact_digest,
        schema_version=schema_version,
        transform_version=transform_version,
    )

    if len(source_batches) == 1:
        single_source = source_batches[0][0]
        manifest_id = hashlib.sha256(
            json.dumps(
                (service_id, domain, single_source.object_key, single_source.checksum, artifact_checksum),
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
    else:
        manifest_id = hashlib.sha256(
            json.dumps(
                (
                    service_id,
                    domain,
                    [s.object_key for s, _ in source_batches],
                    [s.checksum for s, _ in source_batches],
                    artifact_checksum,
                ),
                separators=(",", ":"),
            ).encode()
        ).hexdigest()

    manifest = ArchiveManifest(
        manifest_id=manifest_id,
        service_id=service_id,
        domain=domain,
        sources=tuple(source_entries),
        artifact=artifact,
        coverage_start=coverage_start.astimezone(UTC),
        coverage_end=coverage_end.astimezone(UTC),
        retention_deadline=coverage_end.astimezone(UTC) + timedelta(seconds=retention_seconds),
        deletion_authorization_deadline=deletion_deadline,
        archive_epoch=archive_epoch,
    )
    manifest.validate()
    manifest_path = output_root / f"{manifest_id}.manifest.json"
    atomic_write_json(manifest_path, asdict(manifest), default=_json_default, sort_keys=True, indent=2)
    return manifest


def _parquet_safe_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        if not value:
            return None
        return {key: _parquet_safe_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_parquet_safe_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_parquet_safe_value(item) for item in value)
    return value


_self_mod = sys.modules[__name__]
sys.modules["backend.high_scale.archive_writer"] = _self_mod
_hs_pkg = sys.modules.get("backend.high_scale")
if _hs_pkg is not None:
    setattr(_hs_pkg, "archive_writer", _self_mod)
