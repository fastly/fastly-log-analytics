"""Archive-only rebuild of the Security projection (``security_minute_dimensions``).

Mirrors the Origin projection rebuild: rows are re-read from the immutable FOS
archive, re-enriched against the current NGWAF bot cache, and re-projected under
rebuild-scoped batch ids. A rebuild ADDS rows -- run it over a window whose existing
``security_minute_dimensions`` rows were removed first, or the window is counted twice.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

from backend.high_scale.archive_models import ArchiveManifest
from backend.high_scale.archive_publication import ArchivePublication
from backend.high_scale.ingest_controller import _enrich_high_scale_ngwaf_bots
from backend.high_scale.publication import ClickHousePublication
from backend.high_scale.recovery import ArchiveManifestCatalog, RecoveryReport, _evaluate_capacity_gate, _sorted
from backend.high_scale.recovery_capacity import RecoveryCapacityMeasurement
from backend.high_scale.replay import ProjectionReplayResult, _batch_from_rows, _read_manifest_rows
from backend.high_scale.security_projection import build_security_projection_rows


def replay_security_projections(
    manifest: ArchiveManifest,
    archive: ArchivePublication,
    publication: ClickHousePublication,
    *,
    rebuild_id: str,
) -> ProjectionReplayResult:
    if manifest.source.domain != "request":
        raise ValueError("Security projections can only be rebuilt from request manifests")
    rows = _read_manifest_rows(manifest, archive)
    serving_rows = tuple(row for row in rows if row.get("_record_kind", "event") == "event")
    _enrich_high_scale_ngwaf_bots(serving_rows)
    projection = build_security_projection_rows(serving_rows)
    published = 0
    if projection.dimension_rows:
        result = publication.publish(
            _batch_from_rows(
                f"rebuild-security:{rebuild_id}:{manifest.manifest_id}:dimensions",
                manifest,
                projection.dimension_rows,
                domain="security_dimensions",
            )
        )
        published = result.rows_visible
    return ProjectionReplayResult(manifest.manifest_id, len(serving_rows), published)


def rebuild_security_projections_from_fos(
    catalog: ArchiveManifestCatalog,
    archive: ArchivePublication,
    publication: ClickHousePublication,
    *,
    service_id: str,
    now: datetime | None = None,
    window: timedelta = timedelta(days=30),
    rebuild_id: str | None = None,
    capacity: RecoveryCapacityMeasurement | None = None,
) -> RecoveryReport:
    if window <= timedelta(0):
        raise ValueError("window must be positive")
    observed = (now or datetime.now(UTC)).astimezone(UTC)
    resolved_rebuild_id = rebuild_id or uuid4().hex
    capacity_report = _evaluate_capacity_gate(
        capacity, service_id=service_id, domain="request", rebuild_id=resolved_rebuild_id
    )
    manifests = _sorted(catalog.manifests_covering(service_id, "request", observed - window, observed))
    rows = 0
    replayed = 0
    errors: list[str] = []
    for manifest in manifests:
        try:
            result = replay_security_projections(manifest, archive, publication, rebuild_id=resolved_rebuild_id)
        except Exception as exc:
            errors.append(f"manifest {manifest.manifest_id} failed to rebuild Security projections: {exc}")
            continue
        rows += result.rows_published
        replayed += 1
    available = not errors
    return RecoveryReport(
        ingest_accepted=True,
        triage_available=available,
        raw_query_available=True,
        warm_history_available=available,
        full_rebuild_complete=available,
        manifests_replayed=replayed,
        rows_replayed=rows,
        errors=tuple(errors),
        rebuild_id=resolved_rebuild_id,
        capacity=capacity_report,
        recovery_claim_valid=None if capacity_report is None else capacity_report.feasible and available,
    )
