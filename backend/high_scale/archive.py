"""Typed archive, deployment profile, serving-volume contracts, and verified archive publication for the high-scale plane."""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import re
import sys
import threading
import time
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from functools import cached_property
from pathlib import Path
from threading import Event, Lock, RLock
from typing import Any, Literal, Protocol
from urllib.parse import urlparse
from uuid import UUID, uuid4, uuid5

import pyarrow as pa
import pyarrow.parquet as pq
import structlog
from cachetools import LRUCache

from backend.core.clickhouse import CLICKHOUSE_HIGH_SCALE_TABLES, get_clickhouse_client
from backend.core.clickhouse import ClickHouseClient as HttpClickHouseClient
from backend.utils.structlog_config import atomic_temp_path, atomic_write_json

logger = structlog.get_logger(__name__)

_BASELINE_HOT_DAYS = 1.0
_BASELINE_WARM_DAYS = 30.0
_PROTECTED_RESERVE_FRACTION = 0.20
_WARM_COMPACTION_RATIO = 0.3
_SECONDS_PER_DAY = 86_400


class ArchiveState(StrEnum):
    ARTIFACT_UPLOADING = "artifact_uploading"
    ARTIFACT_VERIFIED = "artifact_verified"
    MANIFEST_PREPARED = "manifest_prepared"
    MANIFEST_COMMITTED = "manifest_committed"
    DELETION_ELIGIBLE = "deletion_eligible"
    SOURCE_DELETED = "source_deleted"


ARCHIVE_STATES = tuple(ArchiveState)


def validate_archive_transition(current: ArchiveState, next_state: ArchiveState) -> None:
    """Reject state regressions and transitions that skip a protocol phase."""
    try:
        current_index, next_index = ARCHIVE_STATES.index(current), ARCHIVE_STATES.index(next_state)
    except ValueError as exc:
        raise ValueError("unknown archive state") from exc
    if next_index != current_index + 1:
        raise ValueError("archive state transitions must be monotonic and adjacent")


@dataclass(frozen=True)
class ServiceProfile:
    events_per_second: float
    average_row_bytes: float = 500.0

    def __post_init__(self) -> None:
        if self.events_per_second <= 0:
            raise ValueError("events_per_second must be positive")
        if self.average_row_bytes <= 0:
            raise ValueError("average_row_bytes must be positive")


@dataclass(frozen=True)
class StorageProfile:
    total_bytes: int
    protected_reserve_fraction: float = _PROTECTED_RESERVE_FRACTION

    def __post_init__(self) -> None:
        if self.total_bytes <= 0:
            raise ValueError("total_bytes must be positive")
        if not 0.0 <= self.protected_reserve_fraction < 1.0:
            raise ValueError("protected_reserve_fraction must be in [0, 1)")


@dataclass(frozen=True)
class RetentionRecommendation:
    hot_days: float
    warm_days: float
    reasoning: str


def recommend_retention(service_profile: ServiceProfile, storage_profile: StorageProfile) -> RetentionRecommendation:
    usable_bytes = storage_profile.total_bytes * (1.0 - storage_profile.protected_reserve_fraction)
    bytes_per_day = service_profile.events_per_second * service_profile.average_row_bytes * _SECONDS_PER_DAY
    hot_days = min(_BASELINE_HOT_DAYS, usable_bytes / bytes_per_day)
    remaining_bytes = max(0.0, usable_bytes - hot_days * bytes_per_day)
    warm_bytes_per_day = bytes_per_day * _WARM_COMPACTION_RATIO
    max_warm_days = remaining_bytes / warm_bytes_per_day if warm_bytes_per_day > 0 else 0.0
    warm_days = min(_BASELINE_WARM_DAYS, hot_days + max_warm_days)
    reasoning = (
        f"{service_profile.events_per_second:.0f} events/s at {service_profile.average_row_bytes:.0f} "
        f"bytes/row against {usable_bytes:.0f} usable bytes ({storage_profile.protected_reserve_fraction:.0%} reserved)"
    )
    return RetentionRecommendation(hot_days=hot_days, warm_days=warm_days, reasoning=reasoning)


@dataclass(frozen=True)
class TopologyProfile:
    name: str
    clickhouse_replicas_per_shard: int
    keeper_members: int
    persistent_serving_storage: bool
    production_ha: bool
    shard_count: int = 1


def build_topology_profile(profile_name: str) -> TopologyProfile:
    if profile_name == "local":
        return TopologyProfile("local", 1, 1, True, False, 1)
    if profile_name == "portable-production":
        return TopologyProfile("portable-production", 2, 3, True, True, 3)
    raise ValueError("unknown high-scale topology profile")


@dataclass(frozen=True)
class HighScaleConfig:
    enabled: bool
    clickhouse_url: str | None
    fos_bucket: str | None
    archive_manifest_dsn: str | None
    worker_queue_url: str | None = None
    persistent_storage: bool = True
    profile: str = "local"

    def topology_profile(self) -> TopologyProfile:
        return _resolve_topology_profile(self.profile)

    def validate(self) -> None:
        if not self.enabled:
            return
        for value, message in (
            (self.clickhouse_url, "high-scale mode requires a ClickHouse URL"),
            (self.fos_bucket, "high-scale mode requires an FOS archive bucket"),
            (self.archive_manifest_dsn, "high-scale mode requires an archive manifest DSN"),
            (self.worker_queue_url, "high-scale mode requires a durable worker queue URL"),
            (self.persistent_storage, "high-scale mode requires persistent serving storage"),
        ):
            if not value:
                raise ValueError(message)
        self.topology_profile()


def _resolve_topology_profile(profile_name: str) -> TopologyProfile:
    """Single source of truth for profile names and replica/Keeper expectations."""
    try:
        return build_topology_profile(profile_name)
    except ValueError:
        raise ValueError("high-scale profile must be local or portable-production") from None


def from_environment(env: Mapping[str, str] | None = None) -> HighScaleConfig:
    source = os.environ if env is None else env
    config = HighScaleConfig(
        enabled=source.get("HIGH_SCALE_ENABLED", "").lower() in {"1", "true", "yes"},
        clickhouse_url=source.get("HIGH_SCALE_CLICKHOUSE_URL"),
        fos_bucket=source.get("HIGH_SCALE_FOS_BUCKET"),
        archive_manifest_dsn=source.get("HIGH_SCALE_ARCHIVE_MANIFEST_DSN"),
        worker_queue_url=source.get("HIGH_SCALE_WORKER_QUEUE_URL"),
        persistent_storage=source.get("HIGH_SCALE_PERSISTENT_STORAGE", "true").lower() not in {"0", "false", "no"},
        profile=source.get("HIGH_SCALE_PROFILE", "local"),
    )
    config.validate()
    return config


def topology_from_environment(env: Mapping[str, str] | None = None) -> TopologyProfile:
    """Resolve and validate the active topology profile from environment variables."""
    source = os.environ if env is None else env
    return _resolve_topology_profile(source.get("HIGH_SCALE_PROFILE", "local"))


# ── Serving-volume disk guard and periodic sampler (folded from disk_sampler.py) ──

DiskMode = Literal["normal", "warning", "protective", "emergency"]

_RESERVE_FRACTION = 0.20
_PROTECTIVE_FREE_FRACTION = 0.15
_EMERGENCY_FREE_FRACTION = 0.10


@dataclass(frozen=True)
class VolumeStats:
    total_bytes: int
    free_bytes: int
    one_batch_bytes: int = 0

    def __post_init__(self) -> None:
        for valid, msg in (
            (self.total_bytes > 0, "total_bytes must be positive"),
            (self.free_bytes >= 0, "free_bytes must be non-negative"),
            (self.free_bytes <= self.total_bytes, "free_bytes cannot exceed total_bytes"),
            (self.one_batch_bytes >= 0, "one_batch_bytes must be non-negative"),
        ):
            if not valid:
                raise ValueError(msg)


@dataclass(frozen=True)
class DiskState:
    mode: DiskMode
    free_fraction: float
    reserve_bytes: int
    reasoning: str


def evaluate_disk_state(volume: VolumeStats) -> DiskState:
    free_fraction = volume.free_bytes / volume.total_bytes
    reserve_bytes = max(int(volume.total_bytes * _RESERVE_FRACTION), volume.one_batch_bytes)
    if free_fraction <= _EMERGENCY_FREE_FRACTION:
        mode: DiskMode = "emergency"
        reasoning = f"free space {free_fraction:.1%} at or below the {_EMERGENCY_FREE_FRACTION:.0%} emergency floor"
    elif free_fraction <= _PROTECTIVE_FREE_FRACTION:
        mode = "protective"
        reasoning = f"free space {free_fraction:.1%} at or below the {_PROTECTIVE_FREE_FRACTION:.0%} protective floor"
    elif volume.free_bytes <= reserve_bytes:
        mode = "warning"
        reasoning = f"free space {volume.free_bytes} bytes at or below the {reserve_bytes}-byte protected reserve"
    else:
        mode = "normal"
        reasoning = f"free space {free_fraction:.1%} clears the {reserve_bytes}-byte protected reserve"
    return DiskState(mode=mode, free_fraction=free_fraction, reserve_bytes=reserve_bytes, reasoning=reasoning)


MODE_LEVELS: dict[DiskMode, int] = {"normal": 0, "warning": 1, "protective": 2, "emergency": 3}


class StorageProbe(Protocol):
    def storage_health(self) -> dict: ...


@dataclass(frozen=True)
class DiskSnapshot:
    state: DiskState
    total_bytes: int
    free_bytes: int
    sampled_at: float


_lock = threading.Lock()
_latest: DiskSnapshot | None = None


def _one_batch_bytes() -> int:
    raw = os.getenv("HIGH_SCALE_DISK_ONE_BATCH_BYTES", "").strip()
    if not raw:
        return 0
    try:
        return max(0, int(raw))
    except ValueError:
        logger.warning("high_scale.disk_batch_bytes_invalid", value=raw)
        return 0


def sample_disk_state(probe: StorageProbe, *, now: float | None = None) -> DiskSnapshot:
    """Sample the serving volume once and record the evaluated mode."""
    health = probe.storage_health()
    volume = VolumeStats(int(health["disk_total"]), int(health["disk_free"]), _one_batch_bytes())
    snapshot = DiskSnapshot(
        evaluate_disk_state(volume), volume.total_bytes, volume.free_bytes, time.time() if now is None else now
    )
    _record(snapshot)
    return snapshot


def _record(snapshot: DiskSnapshot) -> None:
    global _latest
    with _lock:
        previous, _latest = _latest, snapshot
    if previous is None or previous.state.mode != snapshot.state.mode:
        log = logger.warning if MODE_LEVELS[snapshot.state.mode] >= MODE_LEVELS["protective"] else logger.info
        log(
            "high_scale.disk_mode_changed",
            previous_mode=previous.state.mode if previous else None,
            mode=snapshot.state.mode,
            free_fraction=round(snapshot.state.free_fraction, 4),
            reserve_bytes=snapshot.state.reserve_bytes,
            reasoning=snapshot.state.reasoning,
        )


def latest_disk_snapshot() -> DiskSnapshot | None:
    with _lock:
        return _latest


def disk_snapshot_payload(now: float | None = None) -> dict[str, Any] | None:
    """Health-snapshot projection of the latest sample, or None before the first."""
    snapshot = latest_disk_snapshot()
    if snapshot is None:
        return None
    current = time.time() if now is None else now
    return {
        "mode": snapshot.state.mode,
        "free_fraction": snapshot.state.free_fraction,
        "reserve_bytes": snapshot.state.reserve_bytes,
        "total_bytes": snapshot.total_bytes,
        "free_bytes": snapshot.free_bytes,
        "reasoning": snapshot.state.reasoning,
        "sample_age_s": max(0.0, current - snapshot.sampled_at),
    }


def reset_for_tests() -> None:
    global _latest
    with _lock:
        _latest = None


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


_SCHEMA_FILES = [
    "publication_schema.sql",
    "request_schema.sql",
    "rum_schema.sql",
    "cmcd_schema.sql",
    "rum_aggregate_schema.sql",
    "cmcd_aggregate_schema.sql",
    "origin_schema.sql",
    "network_schema.sql",
    "security_schema.sql",
    "performance_schema.sql",
]


def install_clickhouse_schema() -> None:
    """Install the idempotent ClickHouse schema required by high-scale workers."""
    client = get_clickhouse_client()
    if not client:
        return
    sql_root = Path(__file__).with_name("sql")
    for filename in _SCHEMA_FILES:
        path = sql_root / filename
        if not path.exists():
            continue
        content = path.read_text("utf-8")
        content = re.sub(
            r"ReplicatedReplacingMergeTree\((.*?)\)\s*\('/clickhouse/[^']+',\s*'[^']+'\)",
            r"ReplacingMergeTree(\1)",
            content,
        )
        content = re.sub(r"ReplicatedMergeTree\('/clickhouse/[^']+',\s*'[^']+'\)", "MergeTree()", content)
        content = re.sub(
            r"ReplicatedReplacingMergeTree\(([^)]+)\)\n\s*\('[^']+',\s*'[^']+'\)", r"ReplacingMergeTree(\1)", content
        )
        for stmt in (s.strip() for s in content.split(";")):
            if stmt and not stmt.startswith("--"):
                try:
                    client.execute(stmt)
                except Exception as e:
                    logging.getLogger(__name__).warning("Failed to install schema %s: %s", filename, e)


_MAX_CACHED_BATCH_LOCKS = 1024
_PUBLICATION_TABLE = "high_scale_batch_publications"
_PUBLICATION_COLUMNS = (
    "batch_id",
    "service_id",
    "domain",
    "generation",
    "batch_digest",
    "expected_rows",
    "visible_rows",
    "quorum_acked",
    "publication_state",
    "manifest_version",
    "updated_at",
)


class BaseClickHouseBatchAdapter:
    """Shared idempotent insert-behind-a-publication-row mechanics."""

    def __init__(self, client: HttpClickHouseClient) -> None:
        self._client = client
        self._batch_locks: LRUCache[str, threading.Lock] = LRUCache(maxsize=_MAX_CACHED_BATCH_LOCKS)
        self._batch_locks_guard = threading.Lock()

    def delete_batch_rows(self, table: str, batch_id: str) -> None:
        self._client.delete_batch_rows(table, batch_id)

    def _batch_lock(self, batch_uuid: str) -> threading.Lock:
        with self._batch_locks_guard:
            lock = self._batch_locks.get(batch_uuid)
            if lock is None:
                lock = threading.Lock()
                self._batch_locks[batch_uuid] = lock
            return lock

    def _publish_table_rows(
        self,
        *,
        table: str,
        columns: list[str],
        mapped_rows: Sequence[tuple[Any, ...]],
        batch: HighScaleBatch,
        batch_uuid: str,
    ) -> InsertReceipt:
        existing = self._existing_publication(batch, batch_uuid)
        if existing is not None:
            self._validate_publication(existing, batch)
            if existing["publication_state"] == "visible":
                return InsertReceipt(batch.batch_id, len(batch.rows), batch.digest)
            version = int(existing["manifest_version"])
        else:
            self._insert_publication(batch, batch_uuid, version=1, state="pending", visible_rows=0)
            version = 1

        expected = len(batch.rows)
        existing_rows = self._count_rows(table, batch, batch_uuid)
        if existing_rows not in (0, expected):
            self._client.delete_batch_rows(table, batch_uuid)
            raise PartialInsert(existing_rows)
        if existing_rows == 0:
            self._client.insert_rows(table, columns, list(mapped_rows))
            existing_rows = self._count_rows(table, batch, batch_uuid)
        if existing_rows != expected:
            self._client.delete_batch_rows(table, batch_uuid)
            raise PartialInsert(existing_rows)

        self._insert_publication(batch, batch_uuid, version=version + 1, state="visible", visible_rows=expected)
        return InsertReceipt(batch.batch_id, expected, batch.digest)

    def _existing_publication(self, batch: HighScaleBatch, batch_uuid: str) -> dict[str, Any] | None:
        rows = self._client.execute(
            "SELECT batch_id, service_id, domain, generation, batch_digest, expected_rows, "
            "visible_rows, publication_state, manifest_version FROM high_scale_batch_publications FINAL "
            "WHERE service_id={service_id:String} AND domain={domain:String} "
            "AND generation={generation:String} AND batch_id={batch_id:String}",
            {
                "service_id": batch.service_id,
                "domain": batch.domain,
                "generation": batch.generation,
                "batch_id": batch_uuid,
            },
        )
        if not rows:
            return None
        if len(rows) != 1:
            raise ValueError("ClickHouse publication receipt is ambiguous")
        return rows[0]

    def _count_rows(self, table: str, batch: HighScaleBatch, batch_uuid: str) -> int:
        rows = self._client.execute(
            f"SELECT count() AS row_count FROM `{table}` "
            "WHERE service_id={service_id:String} AND batch_id={batch_id:UUID}",
            {"service_id": batch.service_id, "batch_id": batch_uuid},
        )
        if len(rows) != 1 or not isinstance(rows[0].get("row_count"), int):
            raise ValueError("ClickHouse insert receipt is invalid")
        return rows[0]["row_count"]

    def _insert_publication(
        self, batch: HighScaleBatch, batch_uuid: str, *, version: int, state: str, visible_rows: int
    ) -> None:
        row = (
            batch_uuid,
            batch.service_id,
            batch.domain,
            batch.generation,
            batch.digest,
            len(batch.rows),
            visible_rows,
            1 if state == "visible" else 0,
            state,
            version,
            datetime.now(UTC),
        )
        self._client.insert_rows(_PUBLICATION_TABLE, list(_PUBLICATION_COLUMNS), [row])

    @staticmethod
    def _validate_publication(existing: dict[str, Any], batch: HighScaleBatch) -> None:
        if (
            existing.get("service_id") != batch.service_id
            or existing.get("domain") != batch.domain
            or existing.get("generation") != batch.generation
            or existing.get("batch_digest") != batch.digest
            or existing.get("expected_rows") != len(batch.rows)
        ):
            raise ValueError("ClickHouse publication identity or digest mismatch")
        if existing.get("publication_state") not in {"pending", "visible"}:
            raise ValueError("ClickHouse publication state is invalid")


_TEXT_ALIASES: dict[str, tuple[str, ...]] = {
    "country": ("country", "geo_country_code"),
    "browser": ("browser",),
    "os": ("os",),
    "device": ("device",),
    "city": ("city", "geo_city"),
    "region": ("region", "geo_region"),
    "pop": ("pop", "server_pop"),
    "tls": ("tls", "tls_version"),
}
_TTFB_KEYS = ("ttfb", "time_to_first_byte")


def _text(value: Any) -> str:
    return "" if value is None or isinstance(value, bool) else str(value).strip()


def _first_text(row: Mapping[str, Any], keys: tuple[str, ...]) -> str:
    for key in keys:
        if text := _text(row.get(key)):
            return text
    return ""


def _finite(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(str(value).strip())
    except ValueError:
        return None
    return number if math.isfinite(number) else None


def rum_environment(row: Mapping[str, Any]) -> dict[str, Any]:
    """Text environment fields plus ``ttfb`` (None when absent or not finite)."""
    env: dict[str, Any] = {name: _first_text(row, keys) for name, keys in _TEXT_ALIASES.items()}
    env["ttfb"] = next((val for key in _TTFB_KEYS if (val := _finite(row.get(key))) is not None), None)
    return env


def _position(row: Mapping[str, Any], keys: tuple[str, ...]) -> int:
    for key in keys:
        number = _finite(row.get(key))
        if number is not None and number >= 0:
            return int(number)
    return 0


def rum_error_position(row: Mapping[str, Any]) -> tuple[int, int]:
    """``(line, column)`` of a JavaScript error; ``0`` when the beacon did not report one."""
    return _position(row, ("rum_error_line", "error_line")), _position(row, ("rum_error_col", "error_col"))


_BATCH_NAMESPACE = UUID("f3e6bd14-9df9-5f8a-9d92-7a4f1fd36d9a")
_EVENT_NAMESPACE = UUID("8e5d1e37-4e66-5b66-9f4c-73d7c4a3f0d8")
_DOMAIN_TABLES = {
    "request": "request_facts",
    "rum_vitals": "rum_vitals_facts",
    "rum_errors": "rum_error_facts",
    "cmcd": "cmcd_projection_facts",
}
_COMMON_FACT_COLUMNS = (
    "service_id",
    "event_id",
    "event_timestamp",
    "ingest_timestamp",
    "source_object_key",
    "source_object_version",
    "line_ordinal",
    "transform_version",
    "batch_id",
    "publication_state",
)
_RUM_ENV_TAIL_COLUMNS = ("browser", "os", "device", "city", "region", "pop", "tls", "ttfb")
_DOMAIN_COLUMNS = {
    "request": (*_COMMON_FACT_COLUMNS, "country", "client_ip", "url", "custom_fields", "cmcd"),
    "rum_vitals": (
        *_COMMON_FACT_COLUMNS,
        "client_id",
        "request_event_id",
        "metric_name",
        "metric_value",
        "metric_rating",
        "pathname",
        "country",
        *_RUM_ENV_TAIL_COLUMNS,
    ),
    "rum_errors": (
        *_COMMON_FACT_COLUMNS,
        "client_id",
        "request_event_id",
        "error_message",
        "error_file",
        "pathname",
        "country",
        "error_line",
        "error_col",
        *_RUM_ENV_TAIL_COLUMNS,
    ),
    "cmcd": (
        "service_id",
        "projection_key",
        "request_event_id",
        "event_timestamp",
        "batch_id",
        "publication_state",
        "cmcd",
        "country",
        "asn",
    ),
}


class PublicationStatus(StrEnum):
    PENDING = "pending"
    VISIBLE = "visible"


class PartialInsert(RuntimeError):
    def __init__(self, rows_inserted: int) -> None:
        super().__init__(f"partial ClickHouse insert: {rows_inserted} rows")
        self.rows_inserted = rows_inserted


@dataclass(frozen=True)
class HighScaleBatch:
    batch_id: str
    service_id: str
    domain: str
    generation: str
    rows: tuple[dict[str, Any], ...]

    @cached_property
    def digest(self) -> str:
        payload = "\n".join(
            json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=False) for row in self.rows
        )
        return f"sha256:{hashlib.sha256(payload.encode()).hexdigest()}"


@dataclass(frozen=True)
class BatchManifest:
    batch_id: str
    service_id: str
    domain: str
    generation: str
    digest: str
    expected_rows: int
    status: PublicationStatus
    visible_rows: int = 0


@dataclass(frozen=True)
class InsertReceipt:
    batch_id: str
    rows_inserted: int
    digest: str


class BatchManifestStore(Protocol):
    def get(self, batch_id: str) -> BatchManifest | None: ...
    def put(self, manifest: BatchManifest) -> None: ...


class ClickHouseClient(Protocol):
    """Client contract: retries with one batch_id must not duplicate rows."""

    def insert(self, batch: HighScaleBatch) -> InsertReceipt: ...
    def delete_batch_rows(self, table: str, batch_id: str) -> None: ...


class ClickHouseBatchAdapter(BaseClickHouseBatchAdapter):
    """Adapt high-scale batches to the allowlisted ClickHouse HTTP client."""

    def insert(self, batch: HighScaleBatch) -> InsertReceipt:
        batch_uuid = _batch_uuid(batch)
        with self._batch_lock(batch_uuid):
            return self._insert_locked(batch, batch_uuid)

    def _insert_locked(self, batch: HighScaleBatch, batch_uuid: str) -> InsertReceipt:
        table = _DOMAIN_TABLES.get(batch.domain)
        if table is None or table not in CLICKHOUSE_HIGH_SCALE_TABLES:
            raise ValueError(f"unsupported high-scale domain: {batch.domain}")
        if not batch.rows:
            raise ValueError("high-scale batch must contain at least one row")
        return self._publish_table_rows(
            table=table,
            columns=list(_DOMAIN_COLUMNS[batch.domain]),
            mapped_rows=_rows_for_domain(batch, batch_uuid),
            batch=batch,
            batch_uuid=batch_uuid,
        )


def _batch_uuid(batch: HighScaleBatch) -> str:
    return str(uuid5(_BATCH_NAMESPACE, "\0".join((batch.service_id, batch.domain, batch.generation, batch.batch_id))))


def _event_uuid(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be a non-empty string")
    return str(uuid5(_EVENT_NAMESPACE, f"{field}:{value}"))


def _required_string(row: Mapping[str, Any], field: str, *, default: str | None = None) -> str:
    value = row.get(field, default)
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string")
    return value


def _required_nonnegative_int(row: Mapping[str, Any], field: str) -> int:
    value = row.get(field)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return value


def _timestamp(row: Mapping[str, Any], field: str) -> datetime:
    value = row.get(field)
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, datetime.min.time())
    elif isinstance(value, str):
        try:
            parsed = range_value(value)
        except ValueError:
            raise ValueError(f"{field} must be an ISO timestamp") from None
    else:
        raise ValueError(f"{field} must be an ISO timestamp")
    return (parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed).astimezone(UTC)


def _rum_environment_values(row: Mapping[str, Any], *, with_error_position: bool = False) -> tuple[Any, ...]:
    env = rum_environment(row)
    position = rum_error_position(row) if with_error_position else ()
    return (env["country"], *position, *(env[col] for col in _RUM_ENV_TAIL_COLUMNS))


def _optional_asn(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and value > 0 else None


def _string_map(row: Mapping[str, Any], field: str) -> dict[str, str]:
    value = row.get(field) or {}
    if not isinstance(value, Mapping):
        raise ValueError(f"{field} must be a mapping")
    result: dict[str, str] = {}
    for key, item in value.items():
        if not isinstance(key, str) or not isinstance(item, str):
            raise ValueError(f"{field} keys and values must be strings")
        result[key] = item
    return result


def _common_values(batch: HighScaleBatch, row: Mapping[str, Any], batch_uuid: str) -> tuple[Any, ...]:
    event_id = row.get("event_id")
    if not isinstance(event_id, str) or not event_id:
        raise ValueError("event_id must be a non-empty string")
    timestamp = _timestamp(row, "timestamp")
    return (
        batch.service_id,
        _event_uuid(event_id, field="event_id"),
        timestamp,
        _timestamp(row, "ingest_timestamp") if "ingest_timestamp" in row else timestamp,
        _required_string(row, "source_object_key"),
        _required_string(row, "source_object_version"),
        _required_nonnegative_int(row, "line_ordinal"),
        _required_string(row, "transform_version"),
        batch_uuid,
        "visible",
    )


def row_matches_serving_domain(domain: str, row: Mapping[str, Any]) -> bool:
    """Return True if ``row`` carries the shape ``domain``'s fact table needs."""
    if domain in {"rum_vitals", "rum_errors"}:
        kind = row.get("_rum_kind")
        if kind is not None and kind != ("vitals" if domain == "rum_vitals" else "errors"):
            return False
    if domain == "rum_vitals":
        val = row.get("metric_value")
        return isinstance(val, (int, float)) and not isinstance(val, bool) and math.isfinite(val)
    if domain == "rum_errors":
        msg = row.get("error_message")
        return isinstance(msg, str) and bool(msg)
    return True


_CORE_FACT_KEYS = frozenset(
    {
        "event_id",
        "timestamp",
        "ingest_timestamp",
        "source_object_key",
        "source_object_version",
        "line_ordinal",
        "transform_version",
        "country",
        "client_ip",
        "ip",
        "url",
        "custom_fields",
        "cmcd",
        "service_id",
        "domain",
        "raw_json",
        "waf_req_id",
        "ngwaf_bot_verification_state",
    }
)


def _extract_custom_fields(row: Mapping[str, Any]) -> dict[str, str]:
    base = _string_map(row, "custom_fields")
    for k, v in row.items():
        if k not in _CORE_FACT_KEYS and not k.startswith("_"):
            if isinstance(v, (str, int, float, bool)) and v not in (None, ""):
                base[k] = str(v)
    return base


def _rows_for_domain(batch: HighScaleBatch, batch_uuid: str) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []
    for row in batch.rows:
        if not isinstance(row, Mapping):
            raise ValueError("high-scale batch rows must be mappings")
        if batch.domain == "request":
            rows.append(
                _common_values(batch, row, batch_uuid)
                + (
                    _required_string(row, "country", default=""),
                    _required_string({"client_ip": row.get("client_ip") or row.get("ip")}, "client_ip", default=""),
                    _required_string(row, "url", default=""),
                    _extract_custom_fields(row),
                    _string_map(row, "cmcd"),
                )
            )
        elif batch.domain in {"rum_vitals", "rum_errors"}:
            common = _common_values(batch, row, batch_uuid)
            req_id = row.get("request_event_id")
            req_uuid = None if req_id is None else _event_uuid(req_id, field="request_event_id")
            client_id = _required_string(row, "client_id", default="")
            if batch.domain == "rum_vitals":
                val = row.get("metric_value")
                if not isinstance(val, (int, float)) or isinstance(val, bool) or not math.isfinite(val):
                    raise ValueError("metric_value must be finite")
                tail = (
                    _required_string(row, "metric_name"),
                    float(val),
                    _required_string(row, "metric_rating", default=""),
                    _required_string(row, "pathname", default=""),
                    *_rum_environment_values(row),
                )
            else:
                tail = (
                    _required_string(row, "error_message", default=""),
                    _required_string(row, "error_file", default=""),
                    _required_string(row, "pathname", default=""),
                    *_rum_environment_values(row, with_error_position=True),
                )
            rows.append((*common, client_id, req_uuid, *tail))
        elif batch.domain == "cmcd":
            request_event_id = row.get("request_event_id")
            if not isinstance(request_event_id, str) or not request_event_id:
                raise ValueError("request_event_id must be a non-empty string")
            projection_key = row.get("projection_key") or build_cmcd_projection_key(
                {"request_event_id": request_event_id}
            )
            rows.append(
                (
                    batch.service_id,
                    _event_uuid(projection_key, field="projection_key"),
                    _event_uuid(request_event_id, field="request_event_id"),
                    _timestamp(row, "timestamp"),
                    batch_uuid,
                    "visible",
                    _string_map({"cmcd": row.get("fields", row.get("cmcd"))}, "cmcd"),
                    _required_string(row, "country", default=""),
                    _optional_asn(row.get("asn")),
                )
            )
    return rows


class InMemoryBatchManifestStore:
    def __init__(self) -> None:
        self._manifests: dict[str, BatchManifest] = {}

    def get(self, batch_id: str) -> BatchManifest | None:
        return self._manifests.get(batch_id)

    def put(self, manifest: BatchManifest) -> None:
        self._manifests[manifest.batch_id] = manifest


class PostgresBatchManifestStore:
    """Durable manifest storage backed by the high-scale control plane."""

    def __init__(self, control_plane: Any) -> None:
        self._control_plane = control_plane

    def get(self, batch_id: str) -> BatchManifest | None:
        return self._control_plane.get_batch_manifest(batch_id)

    def put(self, manifest: BatchManifest) -> None:
        self._control_plane.put_batch_manifest(manifest)


class ClickHousePublication:
    def __init__(self, manifests: BatchManifestStore, client: ClickHouseClient) -> None:
        self._manifests, self._client = manifests, client

    def publish(self, batch: HighScaleBatch) -> PublicationResult:
        manifest = self._manifests.get(batch.batch_id)
        if manifest is None:
            manifest = BatchManifest(
                batch.batch_id,
                batch.service_id,
                batch.domain,
                batch.generation,
                batch.digest,
                len(batch.rows),
                PublicationStatus.PENDING,
            )
            self._manifests.put(manifest)
        else:
            self._validate_existing(manifest, batch)
            if manifest.status is PublicationStatus.VISIBLE:
                return PublicationResult(batch.batch_id, manifest.status, manifest.visible_rows, True)

        receipt = self._client.insert(batch)
        if receipt.batch_id != batch.batch_id:
            raise ValueError("ClickHouse receipt batch id mismatch")
        if receipt.digest != batch.digest:
            raise ValueError("ClickHouse receipt digest mismatch")
        if receipt.rows_inserted != len(batch.rows):
            raise PartialInsert(receipt.rows_inserted)

        self._manifests.put(
            BatchManifest(
                manifest.batch_id,
                manifest.service_id,
                manifest.domain,
                manifest.generation,
                manifest.digest,
                manifest.expected_rows,
                PublicationStatus.VISIBLE,
                receipt.rows_inserted,
            )
        )
        return PublicationResult(batch.batch_id, PublicationStatus.VISIBLE, receipt.rows_inserted, False)

    @staticmethod
    def _validate_existing(manifest: BatchManifest, batch: HighScaleBatch) -> None:
        if (
            manifest.service_id != batch.service_id
            or manifest.domain != batch.domain
            or manifest.generation != batch.generation
        ):
            raise ValueError("batch identity does not match existing manifest")
        if manifest.digest != batch.digest:
            raise ValueError("batch digest does not match existing manifest")


# ── Folded from backend/high_scale/historical_jobs.py ────────────────────────


class HistoricalJobState(StrEnum):
    """States exposed by the historical job manager."""

    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    FAILED = "failed"
    EXPIRED = "expired"


@dataclass(frozen=True)
class HistoricalJob:
    """Immutable view of a historical job and its bounded result."""

    job_id: str
    plan: QueryPlan
    state: HistoricalJobState
    created_at: datetime
    expires_at: datetime
    terminal_at: datetime | None
    rows: tuple[dict[str, Any], ...]
    row_count: int
    output_bytes: int
    truncated: bool
    error: str | None


@dataclass
class _JobRecord:
    job_id: str
    plan: QueryPlan
    created_at: datetime
    expires_at: datetime
    state: HistoricalJobState = HistoricalJobState.QUEUED
    terminal_at: datetime | None = None
    rows: list[dict[str, Any]] = field(default_factory=list)
    output_bytes: int = 0
    truncated: bool = False
    error: str | None = None
    stop: Event = field(default_factory=Event)
    future: Future[Any] | None = None


class HistoricalJobManager:
    """Runs bounded cold-query producers in a small, caller-owned pool."""

    def __init__(
        self,
        *,
        max_workers: int = 2,
        max_jobs: int = 1000,
        max_rows: int = 50_000,
        ttl: timedelta = timedelta(minutes=15),
        terminal_grace_ttl: timedelta = timedelta(minutes=5),
        max_output_bytes: int = 8 * 1024 * 1024,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        for valid, msg in (
            (max_workers > 0, "max_workers must be positive"),
            (max_jobs > 0, "max_jobs must be positive"),
            (max_rows > 0, "max_rows must be positive"),
            (ttl > timedelta(0), "ttl must be positive"),
            (terminal_grace_ttl >= timedelta(0), "terminal grace ttl must be non-negative"),
            (max_output_bytes > 0, "max_output_bytes must be positive"),
        ):
            if not valid:
                raise ValueError(msg)
        self._max_jobs, self._max_rows, self._ttl = max_jobs, max_rows, ttl
        self._terminal_grace_ttl, self._max_output_bytes = terminal_grace_ttl, max_output_bytes
        self._clock = clock or (lambda: datetime.now(UTC))
        self._jobs: dict[str, _JobRecord] = {}
        self._lock = RLock()
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="historical-job")

    def submit(
        self, plan: QueryPlan, producer: Callable[[], Iterable[Mapping[str, Any]]], *, now: datetime | None = None
    ) -> str:
        """Queue a producer for a validated, bounded cold plan."""
        if plan.tier is not QueryTier.COLD_JOB or not plan.queued or not plan.bounded:
            raise ValueError("historical jobs require a bounded cold query plan")
        plan.validate()
        if plan.max_rows > self._max_rows:
            raise ValueError("query plan exceeds manager row limit")
        observed = self._utc(now or self._clock())
        with self._lock:
            self._purge_locked(observed)
            if len(self._jobs) >= self._max_jobs:
                raise RuntimeError("historical job capacity is full")
            job_id = uuid4().hex
            record = _JobRecord(job_id, plan, observed, observed + self._ttl)
            self._jobs[job_id] = record
            record.future = self._executor.submit(self._run, record, producer)
            return job_id

    def get(self, job_id: str, *, now: datetime | None = None) -> HistoricalJob | None:
        observed = self._utc(now or self._clock())
        with self._lock:
            self._purge_locked(observed)
            record = self._jobs.get(job_id)
            if record is None:
                return None
            if record.state in _ACTIVE_STATES and observed >= record.expires_at:
                self._expire_record_locked(record, observed)
            return self._snapshot(record)

    def cancel(self, job_id: str, *, now: datetime | None = None) -> bool:
        observed = self._utc(now or self._clock())
        with self._lock:
            record = self._jobs.get(job_id)
            if record is None or record.state in _TERMINAL_STATES:
                return False
            if observed >= record.expires_at:
                self._expire_record_locked(record, observed)
                return False
            record.stop.set()
            if record.state is HistoricalJobState.QUEUED:
                self._finish_locked(record, HistoricalJobState.CANCELLED, observed)
            return True

    def shutdown(self, *, wait: bool = True) -> None:
        self._executor.shutdown(wait=wait, cancel_futures=True)

    def _run(self, record: _JobRecord, producer: Callable[[], Iterable[Mapping[str, Any]]]) -> None:
        with self._lock:
            if record.state is not HistoricalJobState.QUEUED:
                return
            if self._expired_locked(record):
                self._expire_record_locked(record, self._utc(self._clock()))
                return
            record.state = HistoricalJobState.RUNNING
        try:
            iterator = iter(producer())
            while True:
                with self._lock:
                    if self._stop_reason_locked(record):
                        return
                    if len(record.rows) == record.plan.max_rows:
                        record.truncated = True
                        self._finish_locked(record, HistoricalJobState.COMPLETED, self._utc(self._clock()))
                        return
                try:
                    item = next(iterator)
                except StopIteration:
                    break
                with self._lock:
                    if self._stop_reason_locked(record):
                        return
                    row = dict(item)
                    row_bytes = len(
                        json.dumps(row, default=str, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
                    )
                    if record.output_bytes + row_bytes > self._max_output_bytes:
                        record.rows, record.output_bytes = [], 0
                        self._finish_locked(
                            record,
                            HistoricalJobState.FAILED,
                            self._utc(self._clock()),
                            error="historical job output exceeds byte limit",
                        )
                        return
                    record.rows.append(row)
                    record.output_bytes += row_bytes
            with self._lock:
                if not self._stop_reason_locked(record):
                    self._finish_locked(record, HistoricalJobState.COMPLETED, self._utc(self._clock()))
        except Exception as exc:
            with self._lock:
                if record.state in _ACTIVE_STATES:
                    self._finish_locked(record, HistoricalJobState.FAILED, self._utc(self._clock()), error=str(exc))

    def _stop_reason_locked(self, record: _JobRecord) -> bool:
        if record.state in {HistoricalJobState.CANCELLED, HistoricalJobState.EXPIRED, HistoricalJobState.FAILED}:
            return True
        if self._expired_locked(record):
            self._expire_record_locked(record, self._utc(self._clock()))
            return True
        if record.stop.is_set():
            self._finish_locked(record, HistoricalJobState.CANCELLED, self._utc(self._clock()))
            return True
        return False

    def _expired_locked(self, record: _JobRecord) -> bool:
        return self._utc(self._clock()) >= record.expires_at

    def _expire_record_locked(self, record: _JobRecord, observed: datetime) -> None:
        if record.state in _ACTIVE_STATES:
            record.stop.set()
            self._finish_locked(record, HistoricalJobState.EXPIRED, observed)

    def _finish_locked(
        self, record: _JobRecord, state: HistoricalJobState, observed: datetime, *, error: str | None = None
    ) -> None:
        if record.state not in _TERMINAL_STATES:
            record.state, record.terminal_at, record.error, record.future = state, observed, error, None

    def _purge_locked(self, observed: datetime) -> None:
        stale = [
            jid
            for jid, rec in self._jobs.items()
            if rec.terminal_at is not None and observed >= rec.terminal_at + self._terminal_grace_ttl
        ]
        for jid in stale:
            del self._jobs[jid]

    def _snapshot(self, record: _JobRecord) -> HistoricalJob:
        rows = tuple(record.rows)
        return HistoricalJob(
            record.job_id,
            record.plan,
            record.state,
            record.created_at,
            record.expires_at,
            record.terminal_at,
            rows,
            len(rows),
            record.output_bytes,
            record.truncated,
            record.error,
        )

    @staticmethod
    def _utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("job timestamps must be timezone-aware")
        return value.astimezone(UTC)


_ACTIVE_STATES = {HistoricalJobState.QUEUED, HistoricalJobState.RUNNING}
_TERMINAL_STATES = {
    HistoricalJobState.COMPLETED,
    HistoricalJobState.CANCELLED,
    HistoricalJobState.FAILED,
    HistoricalJobState.EXPIRED,
}

_default_manager: HistoricalJobManager | None = None
_default_manager_lock = Lock()


def get_historical_job_manager() -> HistoricalJobManager:
    """Return the process-local bounded manager used by the HTTP surface."""
    global _default_manager
    with _default_manager_lock:
        if _default_manager is None:
            _default_manager = HistoricalJobManager()
        return _default_manager


class ExportState(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    FAILED = "failed"
    EXPIRED = "expired"


@dataclass(frozen=True)
class ExportJob:
    job_id: str
    state: ExportState
    rows_written: int
    bytes_written: int
    payload: bytes | None
    error: str | None


@dataclass
class _MutableJob:
    job_id: str
    expires_at: datetime
    state: ExportState = ExportState.QUEUED
    rows_written: int = 0
    bytes_written: int = 0
    payload: bytes | None = None
    error: str | None = None
    cancel_event: threading.Event = field(default_factory=threading.Event)
    future: Future[None] | None = None


class ExportManager:
    def __init__(
        self, *, max_workers: int = 2, max_rows: int = 100_000, max_bytes: int = 50_000_000, ttl_seconds: int = 3_600
    ) -> None:
        if max_workers <= 0 or max_rows <= 0 or max_bytes <= 0 or ttl_seconds <= 0:
            raise ValueError("export limits must be positive")
        self._max_rows, self._max_bytes, self._ttl = max_rows, max_bytes, timedelta(seconds=ttl_seconds)
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="high-scale-export")
        self._jobs: dict[str, _MutableJob] = {}
        self._lock = threading.Lock()

    def submit(self, job_id: str, rows: Iterable[Any], serializer: Callable[[Any], bytes]) -> str:
        with self._lock:
            self._expire_locked(datetime.now(UTC))
            if job_id in self._jobs:
                raise ValueError(f"export job {job_id} already exists")
            job = _MutableJob(job_id=job_id, expires_at=datetime.now(UTC) + self._ttl, cancel_event=threading.Event())
            self._jobs[job_id] = job
            job.future = self._executor.submit(self._run, job, rows, serializer)
        return job_id

    def cancel(self, job_id: str) -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None or job.state in {ExportState.COMPLETED, ExportState.CANCELLED, ExportState.FAILED}:
                return False
            job.cancel_event.set()
            if job.future is not None and job.future.cancel():
                job.state = ExportState.CANCELLED
            return True

    def wait(self, job_id: str) -> ExportJob:
        job = self._get_mutable(job_id)
        if job.future is not None:
            job.future.result()
        return self.get(job_id)

    def get(self, job_id: str) -> ExportJob:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise KeyError(job_id)
            self._expire_locked(datetime.now(UTC))
            return self._snapshot(job)

    def shutdown(self) -> None:
        self._executor.shutdown(wait=True, cancel_futures=True)

    def _run(self, job: _MutableJob, rows: Iterable[Any], serializer: Callable[[Any], bytes]) -> None:
        with self._lock:
            if job.cancel_event.is_set():
                job.state = ExportState.CANCELLED
                return
            job.state = ExportState.RUNNING
        output = bytearray()
        try:
            for row in rows:
                if job.cancel_event.is_set():
                    raise _ExportCancelled
                if job.rows_written >= self._max_rows:
                    raise ValueError("export row limit exceeded")
                encoded = serializer(row)
                if not isinstance(encoded, bytes):
                    raise TypeError("export serializer must return bytes")
                output.extend(encoded + b"\n")
                with self._lock:
                    job.rows_written += 1
                    job.bytes_written = len(output)
                    bytes_written = job.bytes_written
                if bytes_written > self._max_bytes:
                    raise ValueError("export byte limit exceeded")
            with self._lock:
                if job.cancel_event.is_set() or job.state is not ExportState.RUNNING:
                    job.payload = None
                    return
                job.payload, job.state = bytes(output), ExportState.COMPLETED
        except _ExportCancelled:
            with self._lock:
                job.state, job.payload = ExportState.CANCELLED, None
        except Exception as exc:
            with self._lock:
                job.state, job.error, job.payload = ExportState.FAILED, str(exc), None

    def _get_mutable(self, job_id: str) -> _MutableJob:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise KeyError(job_id)
            return job

    def _expire_locked(self, now: datetime) -> None:
        expired_ids: list[str] = []
        terminal = {ExportState.EXPIRED, ExportState.CANCELLED, ExportState.COMPLETED, ExportState.FAILED}
        for job in self._jobs.values():
            if now >= job.expires_at and job.state not in {ExportState.EXPIRED, ExportState.CANCELLED}:
                job.cancel_event.set()
                job.state, job.payload = ExportState.EXPIRED, None
            elif now >= job.expires_at + self._ttl and job.state in terminal:
                expired_ids.append(job.job_id)
        for job_id in expired_ids:
            del self._jobs[job_id]

    @staticmethod
    def _snapshot(job: _MutableJob) -> ExportJob:
        return ExportJob(job.job_id, job.state, job.rows_written, job.bytes_written, job.payload, job.error)


_default_export_manager: ExportManager | None = None
_default_export_manager_lock = threading.Lock()


def get_export_manager() -> ExportManager:
    """Return the process-local bounded manager used by the HTTP surface."""
    global _default_export_manager
    with _default_export_manager_lock:
        if _default_export_manager is None:
            _default_export_manager = ExportManager()
        return _default_export_manager


class _ExportCancelled(Exception):
    pass


# ── Archive-only rebuild of ClickHouse visibility (folded from backend.high_scale.replay) ──


@dataclass(frozen=True)
class ReplayResult:
    manifest_id: str
    rows_read: int
    publication: PublicationResult


@dataclass(frozen=True)
class ProjectionReplayResult:
    manifest_id: str
    rows_read: int
    rows_published: int


def _manifest_domain(manifest: ArchiveManifest) -> str:
    domain = getattr(manifest, "domain", None)
    if domain:
        return str(domain)
    source = getattr(manifest, "source", None)
    return str(source.domain) if source is not None and getattr(source, "domain", None) else "request"


def _manifest_service_id(manifest: ArchiveManifest) -> str:
    service_id = getattr(manifest, "service_id", None)
    if service_id:
        return str(service_id)
    source = getattr(manifest, "source", None)
    return str(source.service_id) if source is not None and getattr(source, "service_id", None) else ""


def replay_manifest(
    manifest: ArchiveManifest,
    archive: ArchivePublication,
    publication: ClickHousePublication,
    *,
    batch_id: str | None = None,
) -> ReplayResult:
    """Replay one archived manifest into ClickHouse."""
    rows = _read_manifest_rows(manifest, archive)
    manifest_domain = _manifest_domain(manifest)
    serving_rows = tuple(
        row
        for row in rows
        if row.get("_record_kind", "event") == "event" and row_matches_serving_domain(manifest_domain, row)
    )
    resolved_batch_id = batch_id or f"replay:{manifest.manifest_id}"
    if not serving_rows:
        return ReplayResult(
            manifest.manifest_id,
            0,
            PublicationResult(
                batch_id=resolved_batch_id, status=PublicationStatus.VISIBLE, rows_visible=0, duplicate=False
            ),
        )
    result = publication.publish(_batch_from_rows(resolved_batch_id, manifest, serving_rows))
    return ReplayResult(manifest.manifest_id, len(serving_rows), result)


def _load_manifest_request_events(
    manifest: ArchiveManifest, archive: ArchivePublication, *, projection_label: str
) -> tuple[dict[str, Any], ...]:
    if _manifest_domain(manifest) != "request":
        raise ValueError(f"{projection_label} projections can only be rebuilt from request manifests")
    return tuple(row for row in _read_manifest_rows(manifest, archive) if row.get("_record_kind", "event") == "event")


def _publish_projection_batches(
    manifest: ArchiveManifest,
    serving_rows: tuple[dict[str, Any], ...],
    publication: ClickHousePublication,
    batches: Iterable[tuple[str, str, tuple[dict[str, Any], ...]]],
) -> ProjectionReplayResult:
    published = sum(
        publication.publish(_batch_from_rows(batch_id, manifest, projected_rows, domain=domain)).rows_visible
        for batch_id, domain, projected_rows in batches
        if projected_rows
    )
    return ProjectionReplayResult(manifest.manifest_id, len(serving_rows), published)


def replay_origin_projections(
    manifest: ArchiveManifest, archive: ArchivePublication, publication: ClickHousePublication, *, rebuild_id: str
) -> ProjectionReplayResult:
    serving_rows = _load_manifest_request_events(manifest, archive, projection_label="Origin")
    projection = build_origin_projection_rows(serving_rows)
    prefix = f"rebuild-origin:{rebuild_id}:{manifest.manifest_id}"
    return _publish_projection_batches(
        manifest,
        serving_rows,
        publication,
        (
            (f"{prefix}:summary", "origin_summary", projection.summary_rows),
            (f"{prefix}:dimensions", "origin_dimensions", projection.dimension_rows),
        ),
    )


def replay_performance_projections(
    manifest: ArchiveManifest, archive: ArchivePublication, publication: ClickHousePublication, *, rebuild_id: str
) -> ProjectionReplayResult:
    """Rebuild the Performance waterfall, scatter and ASN-dimension projections."""
    serving_rows = _load_manifest_request_events(manifest, archive, projection_label="Performance")
    projection = build_performance_projection_rows(serving_rows)
    prefix = f"rebuild-performance:{rebuild_id}:{manifest.manifest_id}"
    asn_rows = tuple(row for row in projection.dimension_rows if row["dimension"] == "asn")
    return _publish_projection_batches(
        manifest,
        serving_rows,
        publication,
        (
            (f"{prefix}:summary", "performance_summary", projection.summary_rows),
            (f"{prefix}:scatter", "performance_scatter", projection.scatter_rows),
            (f"{prefix}:asn", "performance_dimensions", asn_rows),
        ),
    )


def replay_cmcd_projections(
    manifest: ArchiveManifest, archive: ArchivePublication, publication: ClickHousePublication, *, rebuild_id: str
) -> ProjectionReplayResult:
    """Rebuild one request manifest's ``cmcd_projection_facts`` rows from the archive."""
    serving_rows = _load_manifest_request_events(manifest, archive, projection_label="CMCD")
    projected: list[dict[str, Any]] = []
    for row in serving_rows:
        fields = {key: value for key, value in (row.get("cmcd") or {}).items() if value is not None}
        if fields and (projection := project_cmcd({**row, "cmcd": fields})) is not None:
            projected.append(projection)
    return _publish_projection_batches(
        manifest,
        serving_rows,
        publication,
        ((f"rebuild-cmcd:{rebuild_id}:{manifest.manifest_id}", "cmcd", tuple(projected)),),
    )


def _read_manifest_rows(manifest: ArchiveManifest, archive: ArchivePublication) -> tuple[dict[str, Any], ...]:
    payload = archive.read_artifact(manifest)
    rows = tuple(pq.read_table(pa.BufferReader(payload)).to_pylist())
    if len(rows) != manifest.artifact.row_count:
        raise ValueError("replay row count does not match archive manifest")
    if event_digest(rows) != manifest.artifact.canonical_digest:
        raise ValueError("replay event digest does not match archive manifest")
    return rows


def _batch_from_rows(
    batch_id: str, manifest: ArchiveManifest, rows: tuple[dict[str, Any], ...], *, domain: str | None = None
):
    from backend.high_scale.publication import HighScaleBatch

    return HighScaleBatch(
        batch_id,
        _manifest_service_id(manifest),
        domain or _manifest_domain(manifest),
        str(manifest.archive_epoch),
        rows,
    )


_self_mod = sys.modules[__name__]
_hs_pkg = sys.modules.get("backend.high_scale")
for _alias in (
    "archive_models",
    "archive_publication",
    "archive_writer",
    "models",
    "disk_sampler",
    "retention_watermark",
    "publication",
    "schema",
    "_batch_adapter_base",
    "historical_jobs",
    "exports",
    "replay",
):
    sys.modules[f"backend.high_scale.{_alias}"] = _self_mod
    if _hs_pkg is not None:
        setattr(_hs_pkg, _alias, _self_mod)

from backend.high_scale._common import range_value  # noqa: E402
from backend.high_scale.cmcd import project_cmcd  # noqa: E402
from backend.high_scale.origin import build_origin_projection_rows  # noqa: E402
from backend.high_scale.performance import build_performance_projection_rows  # noqa: E402
from backend.high_scale.query_service import QueryPlan, QueryTier  # noqa: E402
