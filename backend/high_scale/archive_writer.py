"""Verified, replayable Parquet checkpoint writer for the high-scale plane."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections.abc import Mapping
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from backend.high_scale.archive_models import (
    ArchiveArtifact,
    ArchiveManifest,
    ArchiveSourceEntry,
    ArchiveSourceObject,
)


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
    with tempfile.NamedTemporaryFile(dir=output_root, suffix=".parquet", delete=False) as temp:
        temp_path = Path(temp.name)
    try:
        table = pa.Table.from_pylist(canonical_events)
        pq.write_table(table, temp_path)
        os.replace(temp_path, artifact_path)
    finally:
        temp_path.unlink(missing_ok=True)

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
    temp_manifest = manifest_path.with_suffix(".tmp")
    temp_manifest.write_text(json.dumps(asdict(manifest), default=_json_default, sort_keys=True, indent=2))
    os.replace(temp_manifest, manifest_path)
    return manifest


def write_archive_checkpoint(
    root: str | Path,
    *,
    service_id: str,
    domain: str,
    source: ArchiveSourceObject,
    events: list[dict[str, Any]],
    archive_epoch: int,
    schema_version: str,
    transform_version: str,
    coverage_start: datetime,
    coverage_end: datetime,
    retention_seconds: int = 0,
    deletion_grace_seconds: int = 900,
) -> ArchiveManifest:
    return write_batch_archive_checkpoint(
        root,
        service_id=service_id,
        domain=domain,
        source_batches=[(source, events)],
        archive_epoch=archive_epoch,
        schema_version=schema_version,
        transform_version=transform_version,
        coverage_start=coverage_start,
        coverage_end=coverage_end,
        retention_seconds=retention_seconds,
        deletion_grace_seconds=deletion_grace_seconds,
    )


def verify_archive_checkpoint(manifest: ArchiveManifest) -> None:
    manifest.validate()
    artifact_path = Path(manifest.artifact.uri.removeprefix("file://"))
    if not artifact_path.is_file():
        raise FileNotFoundError(artifact_path)
    if _sha256(artifact_path) != manifest.artifact.checksum:
        raise ValueError("archive artifact checksum mismatch")
    if pq.read_metadata(artifact_path).num_rows != manifest.artifact.row_count:
        raise ValueError("archive artifact row count mismatch")


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"cannot serialize {type(value).__name__}")


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
