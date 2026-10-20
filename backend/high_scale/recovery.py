"""FOS-authoritative rebuild of ClickHouse serving state.

Rebuild direction is always FOS -> ClickHouse: a lost shard or an empty
local ClickHouse store replays archive manifests through the same
:func:`~backend.high_scale.replay.replay_manifest` path used for a single
lost-acknowledgement recovery, but tier-prioritized so a replacement
instance makes recent triage usable long before cold history finishes.
Does not touch the ingest ledger or raw-deletion authority — a rebuild is
purely additive to serving state.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol
from uuid import uuid4

import structlog

from backend.high_scale.archive_models import ArchiveManifest
from backend.high_scale.archive_publication import ArchivePublication
from backend.high_scale.publication import ClickHousePublication
from backend.high_scale.recovery_capacity import (
    RecoveryCapacityMeasurement,
    RecoveryCapacityReport,
    evaluate_recovery_capacity,
)
from backend.high_scale.replay import (
    replay_cmcd_projections,
    replay_manifest,
    replay_origin_projections,
    replay_performance_projections,
)

logger = structlog.get_logger(__name__)


class ArchiveManifestCatalog(Protocol):
    def manifests_covering(
        self, service_id: str, domain: str, start: datetime, end: datetime
    ) -> tuple[ArchiveManifest, ...]: ...


@dataclass(frozen=True)
class RecoveryReport:
    """Recovery availability is reported per capability, not as one flag.

    A replacement instance can serve recent triage and raw queries while
    warm history is still replaying — collapsing that into a single
    "recovered" bit would hide real, actionable partial progress from an
    operator watching the rebuild.
    """

    ingest_accepted: bool
    triage_available: bool
    raw_query_available: bool
    warm_history_available: bool
    full_rebuild_complete: bool
    manifests_replayed: int
    rows_replayed: int
    errors: tuple[str, ...]
    # Pass back into a resumed call (rebuild_id=report.rebuild_id) to retry
    # this SAME attempt idempotently; omit to start a genuinely new one.
    rebuild_id: str
    # Capacity gate (docs/runbooks/high-scale-capacity-and-recovery.md,
    # "Recovery qualification"): populated only when the caller supplies a
    # measured profile. A recovery CLAIM is valid only when the slowest stage
    # sustains the required replay rate AND the rebuild completed; None means
    # no measurement was supplied, not "feasible".
    capacity: RecoveryCapacityReport | None = None
    recovery_claim_valid: bool | None = None


def _evaluate_capacity_gate(
    measurement: RecoveryCapacityMeasurement | None,
    *,
    service_id: str,
    domain: str,
    rebuild_id: str,
) -> RecoveryCapacityReport | None:
    """Evaluate the measured profile before any replay work starts.

    An invalid measurement raises ``ValueError`` here (operator error, nothing
    replayed yet). An infeasible one never blocks recovery: restoring data
    matters more than the claim, so it is logged and carried on the report.
    """

    if measurement is None:
        return None
    report = evaluate_recovery_capacity(measurement)
    log = logger.info if report.feasible else logger.warning
    log(
        "high_scale.recovery_capacity_gate",
        service_id=service_id,
        domain=domain,
        rebuild_id=rebuild_id,
        feasible=report.feasible,
        limiting_stage=report.limiting_stage,
        limiting_events_per_second=report.limiting_events_per_second,
        required_events_per_second=report.required_events_per_second,
    )
    return report


def rebuild_serving_state_from_fos(
    catalog: ArchiveManifestCatalog,
    archive: ArchivePublication,
    publication: ClickHousePublication,
    *,
    service_id: str,
    domain: str,
    now: datetime | None = None,
    recent_window: timedelta = timedelta(hours=24),
    warm_window: timedelta = timedelta(days=30),
    rebuild_id: str | None = None,
    capacity: RecoveryCapacityMeasurement | None = None,
) -> RecoveryReport:
    if recent_window <= timedelta(0):
        raise ValueError("recent_window must be positive")
    if warm_window <= recent_window:
        raise ValueError("warm_window must exceed recent_window")
    observed = (now or datetime.now(UTC)).astimezone(UTC)
    recent_start = observed - recent_window
    warm_start = observed - warm_window
    # Every batch id is scoped to a rebuild_id, never to replay's bare
    # default. The manifest-visibility bookkeeping a replay relies on for
    # idempotency lives in Postgres and can outlive an independent loss of
    # the actual ClickHouse data (that is the whole scenario this function
    # recovers from) — reusing replay's default batch id would let a stale
    # "already visible" record from an EARLIER, unrelated replay attempt
    # silently skip re-inserting rows that are no longer actually there
    # (found in a live recovery drill; see test_recovery.py's
    # test_recovery_reinserts_rows_whose_manifest_was_already_replayed_once).
    #
    # A caller resuming THIS SAME rebuild attempt (e.g. after a crash mid-
    # rebuild) must pass the SAME rebuild_id back to stay idempotent and
    # avoid duplicating already-inserted rows — omitting it always starts a
    # fresh, un-deduplicated rebuild scope, appropriate only for a genuinely
    # new recovery attempt.
    resolved_rebuild_id = rebuild_id or uuid4().hex
    capacity_report = _evaluate_capacity_gate(
        capacity, service_id=service_id, domain=domain, rebuild_id=resolved_rebuild_id
    )

    recent_manifests = _sorted(catalog.manifests_covering(service_id, domain, recent_start, observed))
    warm_manifests = _sorted(catalog.manifests_covering(service_id, domain, warm_start, recent_start))

    recent_rows, recent_replayed, recent_errors = _replay_all(
        recent_manifests, archive, publication, resolved_rebuild_id
    )
    warm_rows, warm_replayed, warm_errors = _replay_all(warm_manifests, archive, publication, resolved_rebuild_id)

    triage_available = not recent_errors
    warm_available = not warm_errors
    return RecoveryReport(
        ingest_accepted=True,
        triage_available=triage_available,
        raw_query_available=triage_available,
        warm_history_available=warm_available,
        full_rebuild_complete=triage_available and warm_available,
        manifests_replayed=recent_replayed + warm_replayed,
        rows_replayed=recent_rows + warm_rows,
        errors=tuple(recent_errors) + tuple(warm_errors),
        rebuild_id=resolved_rebuild_id,
        capacity=capacity_report,
        recovery_claim_valid=(
            None if capacity_report is None else capacity_report.feasible and triage_available and warm_available
        ),
    )


class _SqlClient(Protocol):
    def execute(self, sql: str, params: dict | None = None) -> list[dict]: ...


_ORIGIN_PROJECTION_TABLES = ("origin_minute_summary", "origin_minute_dimensions")


def purge_origin_projections(client: _SqlClient, *, service_id: str, since: datetime) -> None:
    """Delete one service's Origin projection rows from ``since`` onward, synchronously.

    A rebuild publishes under new batch ids, so rebuilding over rows that already exist would
    count every request twice. Purge first when the existing rows are being replaced (for
    example after a projection column was added); the archive remains the source for the rebuild.
    """
    for table in _ORIGIN_PROJECTION_TABLES:
        client.execute(
            f"ALTER TABLE `{table}` DELETE WHERE service_id={{service_id:String}} "
            "AND bucket_start >= {since:DateTime64(3)} SETTINGS mutations_sync=2",
            {"service_id": service_id, "since": since},
        )


_RUM_FACT_TABLES = {"rum_vitals": "rum_vitals_facts", "rum_errors": "rum_error_facts"}


def purge_rum_facts(client: _SqlClient, *, domain: str, service_id: str, since: datetime) -> None:
    """Delete one service's RUM fact rows of ``domain`` from ``since`` onward, synchronously.

    Used to replace rows written before a fact column existed: a rebuild publishes under new batch
    ids, so rebuilding over the old rows would count every beacon twice. The archive is the source.
    """
    table = _RUM_FACT_TABLES.get(domain)
    if table is None:
        raise ValueError("domain must be rum_vitals or rum_errors")
    client.execute(
        f"ALTER TABLE `{table}` DELETE WHERE service_id={{service_id:String}} "
        "AND event_timestamp >= {since:DateTime64(3)} SETTINGS mutations_sync=2",
        {"service_id": service_id, "since": since},
    )


def rebuild_rum_facts_from_fos(
    catalog: ArchiveManifestCatalog,
    archive: ArchivePublication,
    publication: ClickHousePublication,
    *,
    service_id: str,
    domain: str,
    now: datetime | None = None,
    window: timedelta = timedelta(days=30),
    rebuild_id: str | None = None,
    capacity: RecoveryCapacityMeasurement | None = None,
) -> RecoveryReport:
    """Replay one RUM domain over ``window`` (newest day first) through the fact writer."""
    if domain not in _RUM_FACT_TABLES:
        raise ValueError("domain must be rum_vitals or rum_errors")
    return rebuild_serving_state_from_fos(
        catalog,
        archive,
        publication,
        service_id=service_id,
        domain=domain,
        now=now,
        recent_window=min(timedelta(hours=24), window / 2),
        warm_window=window,
        rebuild_id=rebuild_id,
        capacity=capacity,
    )


def purge_cmcd_projections(client: _SqlClient, *, service_id: str, since: datetime) -> None:
    """Delete one service's CMCD projection rows from ``since`` onward, synchronously.

    Same contract as :func:`purge_origin_projections`: a rebuild publishes under new batch ids, so
    replacing existing rows (for example to populate the geography columns) must purge them first.
    """
    client.execute(
        "ALTER TABLE `cmcd_projection_facts` DELETE WHERE service_id={service_id:String} "
        "AND event_timestamp >= {since:DateTime64(3)} SETTINGS mutations_sync=2",
        {"service_id": service_id, "since": since},
    )


def rebuild_cmcd_projections_from_fos(
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
            result = replay_cmcd_projections(manifest, archive, publication, rebuild_id=resolved_rebuild_id)
        except Exception as exc:
            errors.append(f"manifest {manifest.manifest_id} failed to rebuild CMCD projections: {exc}")
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


def rebuild_origin_projections_from_fos(
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
            result = replay_origin_projections(
                manifest,
                archive,
                publication,
                rebuild_id=resolved_rebuild_id,
            )
        except Exception as exc:
            errors.append(f"manifest {manifest.manifest_id} failed to rebuild Origin projections: {exc}")
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


def rebuild_performance_projections_from_fos(
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
    """Populate the Performance waterfall/scatter projections for already-ingested history.

    Pass ``now`` as the moment the projection-writing worker was deployed: manifests
    ingested after that already published these projections live, and replaying them
    here would double-count the waterfall sums.
    """
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
            result = replay_performance_projections(
                manifest,
                archive,
                publication,
                rebuild_id=resolved_rebuild_id,
            )
        except Exception as exc:
            errors.append(f"manifest {manifest.manifest_id} failed to rebuild Performance projections: {exc}")
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


def _sorted(manifests: tuple[ArchiveManifest, ...]) -> tuple[ArchiveManifest, ...]:
    return tuple(sorted(manifests, key=lambda manifest: manifest.coverage_start))


def _replay_all(
    manifests: tuple[ArchiveManifest, ...],
    archive: ArchivePublication,
    publication: ClickHousePublication,
    rebuild_id: str,
) -> tuple[int, int, list[str]]:
    rows = 0
    replayed = 0
    errors: list[str] = []
    for manifest in manifests:
        try:
            result = replay_manifest(
                manifest, archive, publication, batch_id=f"rebuild:{rebuild_id}:{manifest.manifest_id}"
            )
        except Exception as exc:
            errors.append(f"manifest {manifest.manifest_id} failed to replay: {exc}")
            continue
        rows += result.rows_read
        replayed += 1
    return rows, replayed, errors
