"""Archive-only rebuild of ClickHouse visibility."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from backend.high_scale.archive_models import ArchiveManifest
from backend.high_scale.archive_publication import ArchivePublication
from backend.high_scale.publication import ClickHousePublication, PublicationResult


@dataclass(frozen=True)
class ReplayResult:
    manifest_id: str
    rows_read: int
    publication: PublicationResult


def replay_manifest(
    manifest: ArchiveManifest,
    archive: ArchivePublication,
    publication: ClickHousePublication,
) -> ReplayResult:
    payload = archive.read_artifact(manifest)
    table = pq.read_table(pa.BufferReader(payload))
    rows = tuple(table.to_pylist())
    if len(rows) != manifest.artifact.row_count:
        raise ValueError("replay row count does not match archive manifest")
    if _event_digest(rows) != manifest.artifact.canonical_digest:
        raise ValueError("replay event digest does not match archive manifest")
    batch_id = f"replay:{manifest.manifest_id}"
    batch = _batch_from_rows(batch_id, manifest, rows)
    result = publication.publish(batch)
    return ReplayResult(manifest.manifest_id, len(rows), result)


def _batch_from_rows(batch_id: str, manifest: ArchiveManifest, rows: tuple[dict[str, Any], ...]):
    from backend.high_scale.publication import HighScaleBatch

    return HighScaleBatch(
        batch_id=batch_id,
        service_id=manifest.source.service_id,
        domain=manifest.source.domain,
        generation=str(manifest.archive_epoch),
        rows=rows,
    )


def _event_digest(events: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> str:
    digest = hashlib.sha256()
    for event in events:
        digest.update((json.dumps(event, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode())
    return f"sha256:{digest.hexdigest()}"
