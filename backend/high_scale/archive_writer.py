"""Verified, replayable Parquet checkpoint writer for the high-scale plane."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from backend.high_scale.archive_models import ArchiveArtifact, ArchiveManifest, ArchiveSourceObject


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def _event_digest(events: list[dict[str, Any]]) -> str:
    digest = hashlib.sha256()
    for event in events:
        digest.update((json.dumps(event, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode())
    return f"sha256:{digest.hexdigest()}"


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
) -> ArchiveManifest:
    if source.service_id != service_id or source.domain != domain:
        raise ValueError("source identity does not match archive batch")
    if not events:
        raise ValueError("archive checkpoint cannot be empty")
    if coverage_end < coverage_start:
        raise ValueError("archive coverage end precedes coverage start")
    if archive_epoch < 0:
        raise ValueError("archive epoch must be non-negative")

    output_root = Path(root)
    output_root.mkdir(parents=True, exist_ok=True)
    event_digest = _event_digest(events)
    source_token = hashlib.sha256(f"{source.object_key}:{source.checksum}".encode()).hexdigest()[:16]
    artifact_name = f"{service_id}-{domain}-{source_token}.parquet"
    artifact_path = output_root / artifact_name
    with tempfile.NamedTemporaryFile(dir=output_root, suffix=".parquet", delete=False) as temp:
        temp_path = Path(temp.name)
    try:
        table = pa.Table.from_pylist(events)
        pq.write_table(table, temp_path)
        os.replace(temp_path, artifact_path)
    finally:
        temp_path.unlink(missing_ok=True)

    artifact_checksum = _sha256(artifact_path)
    artifact = ArchiveArtifact(
        uri=artifact_path.as_uri(),
        checksum=artifact_checksum,
        size_bytes=artifact_path.stat().st_size,
        row_count=len(events),
        byte_count=sum(len(json.dumps(event, separators=(",", ":"), ensure_ascii=False).encode()) for event in events),
        canonical_digest=event_digest,
        schema_version=schema_version,
        transform_version=transform_version,
    )
    manifest_id = hashlib.sha256(
        json.dumps(
            (service_id, domain, source.object_key, source.checksum, artifact_checksum), separators=(",", ":")
        ).encode()
    ).hexdigest()
    manifest = ArchiveManifest(
        manifest_id=manifest_id,
        source=source,
        artifact=artifact,
        coverage_start=coverage_start.astimezone(UTC),
        coverage_end=coverage_end.astimezone(UTC),
        retention_deadline=coverage_end.astimezone(UTC),
        deletion_authorization_deadline=coverage_end.astimezone(UTC),
        archive_epoch=archive_epoch,
    )
    manifest.validate()
    manifest_path = output_root / f"{manifest_id}.manifest.json"
    temp_manifest = manifest_path.with_suffix(".tmp")
    temp_manifest.write_text(json.dumps(asdict(manifest), default=_json_default, sort_keys=True, indent=2))
    os.replace(temp_manifest, manifest_path)
    return manifest


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
