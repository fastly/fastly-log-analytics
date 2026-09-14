"""Archive-only rebuild of ClickHouse visibility."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from backend.high_scale.archive_models import ArchiveManifest
from backend.high_scale.archive_publication import ArchivePublication
from backend.high_scale.archive_writer import event_digest
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
    payload = archive.read_artifact(manifest)
    table = pq.read_table(pa.BufferReader(payload))
    rows = tuple(table.to_pylist())
    if len(rows) != manifest.artifact.row_count:
        raise ValueError("replay row count does not match archive manifest")
    if event_digest(rows) != manifest.artifact.canonical_digest:
        raise ValueError("replay event digest does not match archive manifest")
    serving_rows = tuple(row for row in rows if row.get("_record_kind", "event") == "event")
    resolved_batch_id = batch_id or f"replay:{manifest.manifest_id}"
    batch = _batch_from_rows(resolved_batch_id, manifest, serving_rows)
    result = publication.publish(batch)
    return ReplayResult(manifest.manifest_id, len(serving_rows), result)


def _batch_from_rows(batch_id: str, manifest: ArchiveManifest, rows: tuple[dict[str, Any], ...]):
    from backend.high_scale.publication import HighScaleBatch

    return HighScaleBatch(
        batch_id=batch_id,
        service_id=manifest.source.service_id,
        domain=manifest.source.domain,
        generation=str(manifest.archive_epoch),
        rows=rows,
    )
