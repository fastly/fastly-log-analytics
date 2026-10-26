"""Typed archive, deployment profile, serving-volume contracts, and verified archive publication for the high-scale plane."""

from __future__ import annotations

import hashlib
import json
import os
import sys
import threading
import time
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, Protocol
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.parquet as pq
import structlog

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
        current_index = ARCHIVE_STATES.index(current)
        next_index = ARCHIVE_STATES.index(next_state)
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


def recommend_retention(
    service_profile: ServiceProfile,
    storage_profile: StorageProfile,
) -> RetentionRecommendation:
    usable_bytes = storage_profile.total_bytes * (1.0 - storage_profile.protected_reserve_fraction)
    bytes_per_day = service_profile.events_per_second * service_profile.average_row_bytes * _SECONDS_PER_DAY

    max_hot_days = usable_bytes / bytes_per_day
    hot_days = min(_BASELINE_HOT_DAYS, max_hot_days)

    remaining_bytes = max(0.0, usable_bytes - hot_days * bytes_per_day)
    warm_bytes_per_day = bytes_per_day * _WARM_COMPACTION_RATIO
    max_warm_days = remaining_bytes / warm_bytes_per_day if warm_bytes_per_day > 0 else 0.0
    warm_days = min(_BASELINE_WARM_DAYS, hot_days + max_warm_days)

    reasoning = (
        f"{service_profile.events_per_second:.0f} events/s at {service_profile.average_row_bytes:.0f} "
        f"bytes/row against {usable_bytes:.0f} usable bytes "
        f"({storage_profile.protected_reserve_fraction:.0%} reserved)"
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
        if not self.clickhouse_url:
            raise ValueError("high-scale mode requires a ClickHouse URL")
        if not self.fos_bucket:
            raise ValueError("high-scale mode requires an FOS archive bucket")
        if not self.archive_manifest_dsn:
            raise ValueError("high-scale mode requires an archive manifest DSN")
        if not self.worker_queue_url:
            raise ValueError("high-scale mode requires a durable worker queue URL")
        if not self.persistent_storage:
            raise ValueError("high-scale mode requires persistent serving storage")
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


# ---------------------------------------------------------------------------
# Serving-volume disk guard and periodic sampler (folded from disk_sampler.py)
# ---------------------------------------------------------------------------

DiskMode = Literal["normal", "warning", "protective", "emergency"]

_RESERVE_FRACTION = 0.20
_PROTECTIVE_FREE_FRACTION = 0.15
_EMERGENCY_FREE_FRACTION = 0.10


@dataclass(frozen=True)
class VolumeStats:
    total_bytes: int
    free_bytes: int
    # Size of one full compaction/replay batch — the reserve floor is the
    # GREATER of the 20% fraction or this, so a large batch on a small
    # volume can never be scheduled into a reserve too thin to hold it.
    one_batch_bytes: int = 0

    def __post_init__(self) -> None:
        if self.total_bytes <= 0:
            raise ValueError("total_bytes must be positive")
        if self.free_bytes < 0:
            raise ValueError("free_bytes must be non-negative")
        if self.free_bytes > self.total_bytes:
            raise ValueError("free_bytes cannot exceed total_bytes")
        if self.one_batch_bytes < 0:
            raise ValueError("one_batch_bytes must be non-negative")


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
    """Sample the serving volume once and record the evaluated mode.

    Raises whatever the probe raises; callers decide how to degrade.
    """

    health = probe.storage_health()
    volume = VolumeStats(
        total_bytes=int(health["disk_total"]),
        free_bytes=int(health["disk_free"]),
        one_batch_bytes=_one_batch_bytes(),
    )
    snapshot = DiskSnapshot(
        state=evaluate_disk_state(volume),
        total_bytes=volume.total_bytes,
        free_bytes=volume.free_bytes,
        sampled_at=time.time() if now is None else now,
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
        if artifact is None and "artifact" in kwargs:
            artifact = kwargs["artifact"]
        if coverage_start is None and "coverage_start" in kwargs:
            coverage_start = kwargs["coverage_start"]
        if coverage_end is None and "coverage_end" in kwargs:
            coverage_end = kwargs["coverage_end"]
        if retention_deadline is None and "retention_deadline" in kwargs:
            retention_deadline = kwargs["retention_deadline"]
        if deletion_authorization_deadline is None and "deletion_authorization_deadline" in kwargs:
            deletion_authorization_deadline = kwargs["deletion_authorization_deadline"]
        if archive_epoch is None and "archive_epoch" in kwargs:
            archive_epoch = kwargs["archive_epoch"]

        if (
            artifact is None
            or coverage_start is None
            or coverage_end is None
            or retention_deadline is None
            or deletion_authorization_deadline is None
            or archive_epoch is None
        ):
            raise TypeError("ArchiveManifest missing required artifact, coverage, or retention parameters")

        if sources is not None:
            resolved_sources = tuple(sources)
        elif source is not None:
            resolved_sources = (
                ArchiveSourceEntry(
                    key=source.object_key,
                    checksum=source.checksum,
                    size_bytes=source.size_bytes,
                    row_start=0,
                    row_count=artifact.row_count,
                    deletion_deadline=deletion_authorization_deadline,
                    version=source.version,
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
                service_id=resolved_service_id,
                domain=resolved_domain,
                object_key=first.key,
                checksum=first.checksum,
                size_bytes=first.size_bytes,
                version=first.version,
            )

        if resolved_source is None:
            raise TypeError("ArchiveManifest requires either source or non-empty sources")

        object.__setattr__(self, "manifest_id", manifest_id)
        object.__setattr__(self, "artifact", artifact)
        object.__setattr__(self, "coverage_start", coverage_start)
        object.__setattr__(self, "coverage_end", coverage_end)
        object.__setattr__(self, "retention_deadline", retention_deadline)
        object.__setattr__(self, "deletion_authorization_deadline", deletion_authorization_deadline)
        object.__setattr__(self, "archive_epoch", archive_epoch)
        object.__setattr__(self, "service_id", resolved_service_id)
        object.__setattr__(self, "domain", resolved_domain)
        object.__setattr__(self, "sources", resolved_sources)
        object.__setattr__(self, "source", resolved_source)

    def validate(self) -> None:
        if self.coverage_end < self.coverage_start:
            raise ValueError("archive coverage end precedes coverage start")
        if self.retention_deadline < self.coverage_end:
            raise ValueError("retention deadline precedes archive coverage")
        if self.deletion_authorization_deadline < self.retention_deadline:
            raise ValueError("deletion authorization precedes retention deadline")
        if self.archive_epoch < 0:
            raise ValueError("archive epoch must be non-negative")
        if self.artifact.row_count < 0 or self.artifact.byte_count < 0:
            raise ValueError("archive counts must be non-negative")
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


# ---------------------------------------------------------------------------
# Verified archive publication against an injected object store
# (folded from archive_publication.py)
# ---------------------------------------------------------------------------


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
sys.modules["backend.high_scale.archive_publication"] = _self_mod
sys.modules["backend.high_scale.archive_models"] = _self_mod
sys.modules["backend.high_scale.archive_writer"] = _self_mod
sys.modules["backend.high_scale.models"] = _self_mod
sys.modules["backend.high_scale.disk_sampler"] = _self_mod
sys.modules["backend.high_scale.retention_watermark"] = _self_mod
_hs_pkg = sys.modules.get("backend.high_scale")
if _hs_pkg is not None:
    setattr(_hs_pkg, "archive_publication", _self_mod)
    setattr(_hs_pkg, "archive_models", _self_mod)
    setattr(_hs_pkg, "archive_writer", _self_mod)
    setattr(_hs_pkg, "models", _self_mod)
    setattr(_hs_pkg, "disk_sampler", _self_mod)
    setattr(_hs_pkg, "retention_watermark", _self_mod)
