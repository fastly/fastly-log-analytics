"""Typed archive, deployment profile, serving-volume contracts, and verified archive publication for the high-scale plane."""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from backend.utils.telemetry import atomic_temp_path, atomic_write_json

logger = structlog.get_logger("backend.high_scale.archive")


@dataclass(frozen=True)
class ArchiveSourceObject:
    service_id: str
    domain: str
    object_key: str
    checksum: str
    size_bytes: int
    version: str | None = None


@dataclass(frozen=True)
class ArchiveArtifact:
    uri: str
    checksum: str
    size_bytes: int
    row_count: int
    byte_count: int
    canonical_digest: str
    schema_version: str
    transform_version: str


@dataclass(frozen=True)
class ArchiveSourceEntry:
    key: str
    checksum: str
    size_bytes: int
    row_start: int
    row_count: int
    deletion_deadline: datetime
    version: str | None = None

    def __post_init__(self) -> None:
        if isinstance(self.version, datetime):
            for k, v in zip(
                ("version", "row_start", "row_count", "deletion_deadline"),
                (self.row_start, self.row_count, self.deletion_deadline, self.version),
                strict=True,
            ):
                object.__setattr__(self, k, v)


@dataclass(frozen=True, init=False)
class ArchiveManifest:
    manifest_id: str
    artifact: ArchiveArtifact
    coverage_start: datetime
    coverage_end: datetime
    retention_deadline: datetime
    deletion_authorization_deadline: datetime
    archive_epoch: int
    service_id: str
    domain: str
    sources: tuple[ArchiveSourceEntry, ...]
    source: ArchiveSourceObject

    def __init__(
        self,
        manifest_id: str,
        source: ArchiveSourceObject | None = None,
        artifact: ArchiveArtifact | None = None,
        coverage_start: datetime | None = None,
        coverage_end: datetime | None = None,
        retention_deadline: datetime | None = None,
        deletion_authorization_deadline: datetime | None = None,
        archive_epoch: int | None = None,
        *,
        service_id: str | None = None,
        domain: str | None = None,
        sources: tuple[ArchiveSourceEntry, ...] | list[ArchiveSourceEntry] | None = None,
        **kwargs: Any,
    ) -> None:
        explicit = {
            "artifact": artifact,
            "coverage_start": coverage_start,
            "coverage_end": coverage_end,
            "retention_deadline": retention_deadline,
            "deletion_authorization_deadline": deletion_authorization_deadline,
            "archive_epoch": archive_epoch,
        }
        resolved = {k: (v if v is not None else kwargs.get(k)) for k, v in explicit.items()}
        if any(v is None for v in resolved.values()):
            raise TypeError("ArchiveManifest missing required artifact, coverage, or retention parameters")

        art: ArchiveArtifact = resolved["artifact"]  # type: ignore[assignment]
        del_deadline: datetime = resolved["deletion_authorization_deadline"]  # type: ignore[assignment]
        if sources is not None:
            resolved_sources = tuple(sources)
        elif source is not None:
            resolved_sources = (
                ArchiveSourceEntry(
                    source.object_key,
                    source.checksum,
                    source.size_bytes,
                    0,
                    art.row_count,
                    del_deadline,
                    source.version,
                ),
            )
        else:
            resolved_sources = ()

        resolved_service_id = service_id or (source.service_id if source else "")
        resolved_domain = domain or (source.domain if source else "request")
        resolved_source = source
        if resolved_source is None and resolved_sources and resolved_service_id:
            first = resolved_sources[0]
            resolved_source = ArchiveSourceObject(
                resolved_service_id, resolved_domain, first.key, first.checksum, first.size_bytes, first.version
            )
        if resolved_source is None:
            raise TypeError("ArchiveManifest requires either source or non-empty sources")

        for name, val in {
            "manifest_id": manifest_id,
            **resolved,
            "service_id": resolved_service_id,
            "domain": resolved_domain,
            "sources": resolved_sources,
            "source": resolved_source,
        }.items():
            object.__setattr__(self, name, val)

    def validate(self) -> None:
        for valid, msg in (
            (self.coverage_end >= self.coverage_start, "archive coverage end precedes coverage start"),
            (self.retention_deadline >= self.coverage_end, "retention deadline precedes archive coverage"),
            (
                self.deletion_authorization_deadline >= self.retention_deadline,
                "deletion authorization precedes retention deadline",
            ),
            (self.archive_epoch >= 0, "archive epoch must be non-negative"),
            (self.artifact.row_count >= 0 and self.artifact.byte_count >= 0, "archive counts must be non-negative"),
        ):
            if not valid:
                raise ValueError(msg)
        total_source_rows = sum(s.row_count for s in self.sources)
        if total_source_rows > self.artifact.row_count:
            raise ValueError(
                f"total source row count {total_source_rows} exceeds artifact row_count {self.artifact.row_count}"
            )

    def is_source_eligible_for_deletion(self, object_key: str, now: datetime | None = None) -> bool:
        if now is None:
            now = datetime.now(self.deletion_authorization_deadline.tzinfo)
        for s in self.sources:
            if s.key == object_key:
                return now >= s.deletion_deadline
        raise KeyError(f"source key {object_key} not found in manifest")


@dataclass(frozen=True)
class ServingWatermark:
    service_id: str
    domain: str
    owner_epoch: int
    coverage_start: datetime | None
    coverage_end: datetime | None
    last_accepted_cursor: str | None
    last_archived_event_id: str | None
    last_visible_event_id: str | None
    exact: bool

    def validate(self) -> None:
        if self.owner_epoch < 0:
            raise ValueError("owner epoch must be non-negative")
        if self.coverage_start and self.coverage_end and self.coverage_end < self.coverage_start:
            raise ValueError("watermark coverage end precedes coverage start")


# ── Verified archive publication against an injected object store ──


class ObjectStore(Protocol):
    def put(self, key: str, payload: bytes) -> None: ...
    def get(self, key: str) -> bytes: ...
    def exists(self, key: str) -> bool: ...
    def delete(self, key: str) -> None: ...


class PublicationState(StrEnum):
    ARTIFACT_VERIFIED = "artifact_verified"
    MANIFEST_PREPARED = "manifest_prepared"
    MANIFEST_COMMITTED = "manifest_committed"


@dataclass(frozen=True, init=False)
class PublicationResult:
    batch_id: str = ""
    status: Any = None
    rows_visible: int = 0
    duplicate: bool = False
    manifest_id: str = ""
    state: Any = None
    artifact_key: str = ""
    manifest_key: str = ""
    commit_key: str = ""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        names = (
            ("manifest_id", "state", "artifact_key", "manifest_key", "commit_key")
            if ("manifest_id" in kwargs or len(args) == 5)
            else ("batch_id", "status", "rows_visible", "duplicate")
        )
        for k, v in {**dict(zip(names, args, strict=False)), **kwargs}.items():
            object.__setattr__(self, k, v)


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
        self._client, self._bucket, self._prefix = client, bucket, prefix.strip("/")

    def put(self, key: str, payload: bytes) -> None:
        self._client.put_object(Bucket=self._bucket, Key=self._key(key), Body=payload)

    def get(self, key: str) -> bytes:
        return self._client.get_object(Bucket=self._bucket, Key=self._key(key))["Body"].read()

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

    def _put_verified(self, key: str, payload: bytes, label: str) -> None:
        self._store.put(key, payload)
        if self._store.get(key) != payload:
            raise ValueError(f"{label} verification failed after upload")

    def expire(self, manifest: ArchiveManifest) -> None:
        """Delete a manifest's FOS objects: the commit marker first so a partial deletion never looks committed.

        Deleting a missing key is a no-op, so an interrupted expiry can simply be run again.
        """
        for key in (
            f"archive/manifests/{manifest.manifest_id}.commit",
            f"archive/manifests/{manifest.manifest_id}.json",
            _artifact_key(manifest.artifact.uri),
        ):
            self._store.delete(key)

    def publish(self, manifest: ArchiveManifest, artifact: bytes) -> PublicationResult:
        manifest.validate()
        artifact_key = _artifact_key(manifest.artifact.uri)
        manifest_key = f"archive/manifests/{manifest.manifest_id}.json"
        commit_key = f"archive/manifests/{manifest.manifest_id}.commit"
        if _checksum(artifact) != manifest.artifact.checksum:
            raise ValueError("artifact checksum does not match manifest")
        if len(artifact) != manifest.artifact.size_bytes:
            raise ValueError("artifact size does not match manifest")

        if self.is_replayable(manifest.manifest_id):
            return PublicationResult(
                manifest.manifest_id, PublicationState.MANIFEST_COMMITTED, artifact_key, manifest_key, commit_key
            )

        self._put_verified(artifact_key, artifact, "artifact")
        manifest_payload = _json_bytes(asdict(manifest))
        self._put_verified(manifest_key, manifest_payload, "manifest")
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
            manifest.manifest_id, PublicationState.MANIFEST_COMMITTED, artifact_key, manifest_key, commit_key
        )

    def state(self, manifest_id: str) -> PublicationState | None:
        commit_key = f"archive/manifests/{manifest_id}.commit"
        if not self._store.exists(commit_key) or not self.is_replayable(manifest_id):
            return None
        return PublicationState.MANIFEST_COMMITTED

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
            if (
                commit.get("manifest_id") != manifest_id
                or commit.get("manifest_checksum") != _checksum(manifest_payload)
                or commit.get("artifact_checksum") != manifest.artifact.checksum
            ):
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
        return f"{parsed.netloc}/{key}" if parsed.scheme == "s3" and parsed.netloc and key else (parsed.netloc or key)
    return uri.removeprefix("file://").lstrip("/")


def _manifest_from_json(payload: bytes) -> ArchiveManifest:
    raw = json.loads(payload)
    raw_sources = raw.get("sources")
    sources = (
        [
            ArchiveSourceEntry(
                s["key"],
                s["checksum"],
                s["size_bytes"],
                s["row_start"],
                s["row_count"],
                _parse_datetime(s["deletion_deadline"]),
                s.get("version"),
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
    if not source_batches or sum(len(events) for _, events in source_batches) == 0:
        raise ValueError("archive checkpoint cannot be empty")
    if any(source.service_id != service_id or source.domain != domain for source, _ in source_batches):
        raise ValueError("source identity does not match archive batch")
    if coverage_end < coverage_start:
        raise ValueError("archive coverage end precedes coverage start")
    if archive_epoch < 0:
        raise ValueError("archive epoch must be non-negative")
    if retention_seconds < 0 or deletion_grace_seconds < 0:
        raise ValueError("retention and deletion grace periods must be non-negative")

    output_root = Path(root)
    output_root.mkdir(parents=True, exist_ok=True)
    all_events: list[dict[str, Any]] = []
    source_entries: list[ArchiveSourceEntry] = []
    current_offset = 0
    cov_end_utc = coverage_end.astimezone(UTC)
    deletion_deadline = cov_end_utc + timedelta(seconds=retention_seconds + deletion_grace_seconds)

    for source, events in source_batches:
        row_count = len(events)
        source_entries.append(
            ArchiveSourceEntry(
                source.object_key,
                source.checksum,
                source.size_bytes,
                current_offset,
                row_count,
                deletion_deadline,
                source.version,
            )
        )
        current_offset += row_count
        all_events.extend(events)

    keys = set().union(*(event.keys() for event in all_events))
    canonical_events = [{key: _parquet_safe_value(event.get(key)) for key in keys} for event in all_events]
    artifact_digest = event_digest(canonical_events)
    source_token = hashlib.sha256(
        ":".join(f"{s.object_key}:{s.checksum}" for s, _ in source_batches).encode()
    ).hexdigest()[:16]

    artifact_path = output_root / f"{service_id}-{domain}-{source_token}.parquet"
    with atomic_temp_path(artifact_path, suffix=".parquet") as temp_path:
        pq.write_table(pa.Table.from_pylist(canonical_events), temp_path)

    artifact_checksum = _sha256(artifact_path)
    byte_count = sum(
        len(json.dumps(event, separators=(",", ":"), ensure_ascii=False).encode()) for event in canonical_events
    )
    artifact = ArchiveArtifact(
        artifact_path.as_uri(),
        artifact_checksum,
        artifact_path.stat().st_size,
        len(all_events),
        byte_count,
        artifact_digest,
        schema_version,
        transform_version,
    )

    manifest_identity = (
        (service_id, domain, source_batches[0][0].object_key, source_batches[0][0].checksum, artifact_checksum)
        if len(source_batches) == 1
        else (
            service_id,
            domain,
            [s.object_key for s, _ in source_batches],
            [s.checksum for s, _ in source_batches],
            artifact_checksum,
        )
    )
    manifest_id = hashlib.sha256(json.dumps(manifest_identity, separators=(",", ":")).encode()).hexdigest()

    manifest = ArchiveManifest(
        manifest_id=manifest_id,
        service_id=service_id,
        domain=domain,
        sources=tuple(source_entries),
        artifact=artifact,
        coverage_start=coverage_start.astimezone(UTC),
        coverage_end=cov_end_utc,
        retention_deadline=cov_end_utc + timedelta(seconds=retention_seconds),
        deletion_authorization_deadline=deletion_deadline,
        archive_epoch=archive_epoch,
    )
    manifest.validate()
    atomic_write_json(
        output_root / f"{manifest_id}.manifest.json", asdict(manifest), default=_json_default, sort_keys=True, indent=2
    )
    return manifest


def _parquet_safe_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _parquet_safe_value(item) for key, item in value.items()} if value else None
    if isinstance(value, list):
        return [_parquet_safe_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_parquet_safe_value(item) for item in value)
    return value


# ── Folded from backend/high_scale/publication.py ────────────────────────────

IDENTITY_FIELDS = (
    "service_id",
    "domain",
    "source_object_key",
    "source_object_version",
    "line_ordinal",
    "transform_version",
)


def _sha256_uuid(payload: str) -> str:
    return str(uuid.UUID(bytes=hashlib.sha256(payload.encode("utf-8")).digest()[:16]))


def build_event_id(row: Mapping[str, Any], *, domain: str | None = None) -> str:
    values = dict(row)
    if domain is not None:
        values["domain"] = domain
    missing = [fld for fld in IDENTITY_FIELDS if fld not in values]
    if missing:
        raise ValueError(f"event identity missing fields: {', '.join(missing)}")
    if not isinstance(values["line_ordinal"], int) or values["line_ordinal"] < 0:
        raise ValueError("line_ordinal must be a non-negative integer")
    if not all(isinstance(values[fld], str) and values[fld] for fld in IDENTITY_FIELDS if fld != "line_ordinal"):
        raise ValueError("event identity string fields must be non-empty strings")
    return _sha256_uuid("|".join(f"{len(str(values[fld]))}:{values[fld]}" for fld in IDENTITY_FIELDS))


def build_request_event_id(row: Mapping[str, Any]) -> str:
    return build_event_id(row, domain="request")


def build_rum_event_id(row: Mapping[str, Any], *, rum_kind: str, sub_ordinal: int | None = None) -> str:
    if rum_kind not in {"vitals", "errors"}:
        raise ValueError("rum_kind must be 'vitals' or 'errors'")
    resolved_sub = sub_ordinal if sub_ordinal is not None else row.get("sub_ordinal")
    domain_tag = (
        f"rum:{rum_kind}:{resolved_sub}" if isinstance(resolved_sub, int) and resolved_sub > 0 else f"rum:{rum_kind}"
    )
    return build_event_id(row, domain=domain_tag)


def build_cmcd_projection_key(row: Mapping[str, Any]) -> str:
    request_event_id = row.get("request_event_id")
    if not isinstance(request_event_id, str) or not request_event_id:
        raise ValueError("CMCD projection requires request_event_id")
    return _sha256_uuid(f"4:cmcd|{len(request_event_id)}:{request_event_id}")


def build_batch_id_from_manifest_id(manifest_id: str) -> str:
    if not isinstance(manifest_id, str) or not manifest_id:
        raise ValueError("manifest_id must be a non-empty string")
    return _sha256_uuid(f"8:manifest|{len(manifest_id)}:{manifest_id}")
