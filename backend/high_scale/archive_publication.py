"""Verified archive publication against an injected object store."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, Protocol
from urllib.parse import urlparse

from backend.high_scale.archive_models import ArchiveArtifact, ArchiveManifest, ArchiveSourceObject


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

    def is_replayable(self, manifest_id: str) -> bool:
        manifest_key = f"archive/manifests/{manifest_id}.json"
        commit_key = f"archive/manifests/{manifest_id}.commit"
        if not self._store.exists(manifest_key) or not self._store.exists(commit_key):
            return False
        try:
            manifest_payload = self._store.get(manifest_key)
            commit = json.loads(self._store.get(commit_key))
            manifest = _manifest_from_json(manifest_payload)
            manifest.validate()
            if commit.get("manifest_id") != manifest_id:
                return False
            if commit.get("manifest_checksum") != _checksum(manifest_payload):
                return False
            if commit.get("artifact_checksum") != manifest.artifact.checksum:
                return False
            artifact = self._store.get(_artifact_key(manifest.artifact.uri))
            return _checksum(artifact) == manifest.artifact.checksum and len(artifact) == manifest.artifact.size_bytes
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            return False


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
    return ArchiveManifest(
        manifest_id=raw["manifest_id"],
        source=ArchiveSourceObject(**raw["source"]),
        artifact=ArchiveArtifact(**raw["artifact"]),
        coverage_start=_parse_datetime(raw["coverage_start"]),
        coverage_end=_parse_datetime(raw["coverage_end"]),
        retention_deadline=_parse_datetime(raw["retention_deadline"]),
        deletion_authorization_deadline=_parse_datetime(raw["deletion_authorization_deadline"]),
        archive_epoch=raw["archive_epoch"],
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
