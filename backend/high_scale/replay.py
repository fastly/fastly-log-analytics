"""Archive-only rebuild of ClickHouse visibility."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from backend.high_scale.archive_models import ArchiveManifest
from backend.high_scale.archive_publication import ArchivePublication
from backend.high_scale.archive_writer import event_digest
from backend.high_scale.origin_projection import build_origin_projection_rows
from backend.high_scale.publication import ClickHousePublication, PublicationResult


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


def replay_manifest(
    manifest: ArchiveManifest,
    archive: ArchivePublication,
    publication: ClickHousePublication,
    *,
    batch_id: str | None = None,
) -> ReplayResult:
    """Replay one archived manifest into ClickHouse.

    ``batch_id`` defaults to a stable ``replay:{manifest_id}`` id, so a
    genuine retry of the SAME replay attempt is correctly treated as a
    duplicate by :meth:`ClickHousePublication.publish`. Pass an explicit,
    run-scoped ``batch_id`` (e.g. from :func:`~backend.high_scale.recovery.
    rebuild_serving_state_from_fos`) for a disaster-recovery rebuild: the
    manifest-visibility bookkeeping lives in Postgres and can outlive an
    independent loss of the actual ClickHouse data, so trusting the default
    id's "already visible" record would silently skip re-inserting rows
    that are no longer actually there.
    """
    rows = _read_manifest_rows(manifest, archive)
    serving_rows = tuple(row for row in rows if row.get("_record_kind", "event") == "event")
    resolved_batch_id = batch_id or f"replay:{manifest.manifest_id}"
    batch = _batch_from_rows(resolved_batch_id, manifest, serving_rows)
    result = publication.publish(batch)
    return ReplayResult(manifest.manifest_id, len(serving_rows), result)


def replay_origin_projections(
    manifest: ArchiveManifest,
    archive: ArchivePublication,
    publication: ClickHousePublication,
    *,
    rebuild_id: str,
) -> ProjectionReplayResult:
    if manifest.source.domain != "request":
        raise ValueError("Origin projections can only be rebuilt from request manifests")
    rows = _read_manifest_rows(manifest, archive)
    serving_rows = tuple(row for row in rows if row.get("_record_kind", "event") == "event")
    projection = build_origin_projection_rows(serving_rows)
    published = 0
    for suffix, domain, projected_rows in (
        ("summary", "origin_summary", projection.summary_rows),
        ("dimensions", "origin_dimensions", projection.dimension_rows),
    ):
        if not projected_rows:
            continue
        result = publication.publish(
            _batch_from_rows(
                f"rebuild-origin:{rebuild_id}:{manifest.manifest_id}:{suffix}",
                manifest,
                projected_rows,
                domain=domain,
            )
        )
        published += result.rows_visible
    return ProjectionReplayResult(manifest.manifest_id, len(serving_rows), published)


def _read_manifest_rows(
    manifest: ArchiveManifest,
    archive: ArchivePublication,
) -> tuple[dict[str, Any], ...]:
    payload = archive.read_artifact(manifest)
    table = pq.read_table(pa.BufferReader(payload))
    rows = tuple(table.to_pylist())
    if len(rows) != manifest.artifact.row_count:
        raise ValueError("replay row count does not match archive manifest")
    if event_digest(rows) != manifest.artifact.canonical_digest:
        raise ValueError("replay event digest does not match archive manifest")
    return rows


def _batch_from_rows(
    batch_id: str,
    manifest: ArchiveManifest,
    rows: tuple[dict[str, Any], ...],
    *,
    domain: str | None = None,
):
    from backend.high_scale.publication import HighScaleBatch

    return HighScaleBatch(
        batch_id=batch_id,
        service_id=manifest.source.service_id,
        domain=domain or manifest.source.domain,
        generation=str(manifest.archive_epoch),
        rows=rows,
    )
