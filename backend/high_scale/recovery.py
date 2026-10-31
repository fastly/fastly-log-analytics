"""FOS-authoritative recovery for high-scale."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol
from uuid import uuid4

import structlog

from backend.high_scale._common import ArchiveManifestCatalog
from backend.high_scale.archive import (
    ArchiveManifest,
    ArchivePublication,
    ClickHousePublication,
    ProjectionReplayResult,
    _batch_from_rows,
    _load_manifest_request_events,
    _read_manifest_rows,
    replay_cmcd_projections,
    replay_manifest,
    replay_origin_projections,
    replay_performance_projections,
    row_matches_serving_domain,
)


@dataclass(frozen=True)
class RecoveryCapacityMeasurement:
    events_per_second: int
    recovery_window_seconds: int
    archive_read_events_per_second: int
    decode_events_per_second: int
    aggregate_events_per_second: int
    clickhouse_insert_events_per_second: int
    replication_factor: int
    live_ingest_reservation: float

    def validate(self) -> None:
        if self.events_per_second <= 0 or self.recovery_window_seconds <= 0:
            raise ValueError("event rate and recovery window must be positive")
        rates = (
            self.archive_read_events_per_second,
            self.decode_events_per_second,
            self.aggregate_events_per_second,
            self.clickhouse_insert_events_per_second,
        )
        if min(rates) <= 0:
            raise ValueError("all recovery throughput measurements must be positive")
        if self.replication_factor < 1:
            raise ValueError("replication factor must be positive")
        if not 0 <= self.live_ingest_reservation < 1:
            raise ValueError("live ingest reservation must be in [0, 1)")


@dataclass(frozen=True)
class RecoveryCapacityReport:
    required_events_per_second: float
    limiting_stage: str
    limiting_events_per_second: int
    replication_adjusted_insert_events_per_second: float
    feasible: bool


def evaluate_recovery_capacity(measurement: RecoveryCapacityMeasurement) -> RecoveryCapacityReport:
    """Evaluate a measured recovery profile without claiming unmeasured capacity."""
    measurement.validate()
    required = (
        measurement.events_per_second
        * 86_400
        / measurement.recovery_window_seconds
        / (1 - measurement.live_ingest_reservation)
    )
    stages = {
        "archive_read": measurement.archive_read_events_per_second,
        "decode": measurement.decode_events_per_second,
        "aggregate_rebuild": measurement.aggregate_events_per_second,
        "clickhouse_insert": measurement.clickhouse_insert_events_per_second // measurement.replication_factor,
    }
    limiting_stage, limiting_rate = min(stages.items(), key=lambda item: item[1])
    replication_adjusted = measurement.clickhouse_insert_events_per_second / measurement.replication_factor
    return RecoveryCapacityReport(
        required, limiting_stage, limiting_rate, replication_adjusted, limiting_rate >= required
    )


@dataclass(frozen=True)
class RecoveryReport:
    """Recovery availability is reported per capability, not as one flag."""

    ingest_accepted: bool
    triage_available: bool
    raw_query_available: bool
    warm_history_available: bool
    full_rebuild_complete: bool
    manifests_replayed: int
    rows_replayed: int
    errors: tuple[str, ...]
    rebuild_id: str
    capacity: RecoveryCapacityReport | None = None
    recovery_claim_valid: bool | None = None


def _evaluate_capacity_gate(
    measurement: RecoveryCapacityMeasurement | None, *, service_id: str, domain: str, rebuild_id: str
) -> RecoveryCapacityReport | None:
    """Evaluate the measured profile before any replay work starts."""
    if measurement is None:
        return None
    report = evaluate_recovery_capacity(measurement)
    rec_logger = structlog.get_logger(__name__)
    log = rec_logger.info if report.feasible else rec_logger.warning
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
    aggregates_publication: ClickHousePublication | None = None,
) -> RecoveryReport:
    if recent_window <= timedelta(0):
        raise ValueError("recent_window must be positive")
    if warm_window <= recent_window:
        raise ValueError("warm_window must exceed recent_window")
    observed = (now or datetime.now(UTC)).astimezone(UTC)
    recent_start, warm_start = observed - recent_window, observed - warm_window
    resolved_rebuild_id = rebuild_id or uuid4().hex
    capacity_report = _evaluate_capacity_gate(
        capacity, service_id=service_id, domain=domain, rebuild_id=resolved_rebuild_id
    )
    recent_manifests = _sorted(catalog.manifests_covering(service_id, domain, recent_start, observed))
    warm_manifests = _sorted(catalog.manifests_covering(service_id, domain, warm_start, recent_start))
    recent_rows, recent_replayed, recent_errors = _replay_all(
        recent_manifests,
        archive,
        publication,
        resolved_rebuild_id,
        aggregates_publication=aggregates_publication,
    )
    warm_rows, warm_replayed, warm_errors = _replay_all(
        warm_manifests,
        archive,
        publication,
        resolved_rebuild_id,
        aggregates_publication=aggregates_publication,
    )
    triage_available, warm_available = not recent_errors, not warm_errors
    recovery_claim_valid = (
        None if capacity_report is None else capacity_report.feasible and triage_available and warm_available
    )
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
        recovery_claim_valid=recovery_claim_valid,
    )


class _SqlClient(Protocol):
    def execute(self, sql: str, params: dict | None = None) -> list[dict]: ...


_ORIGIN_PROJECTION_TABLES = ("origin_minute_summary", "origin_minute_dimensions")
_RUM_FACT_TABLES = {"rum_vitals": "rum_vitals_facts", "rum_errors": "rum_error_facts"}


def _purge_since(client: _SqlClient, table: str, ts_col: str, service_id: str, since: datetime) -> None:
    client.execute(
        f"ALTER TABLE `{table}` DELETE WHERE service_id={{service_id:String}} "
        f"AND {ts_col} >= {{since:DateTime64(3)}} SETTINGS mutations_sync=2",
        {"service_id": service_id, "since": since},
    )


def purge_origin_projections(client: _SqlClient, *, service_id: str, since: datetime) -> None:
    """Delete one service's Origin projection rows from ``since`` onward, synchronously."""
    for table in _ORIGIN_PROJECTION_TABLES:
        _purge_since(client, table, "bucket_start", service_id, since)


def purge_rum_facts(client: _SqlClient, *, domain: str, service_id: str, since: datetime) -> None:
    """Delete one service's RUM fact rows of ``domain`` from ``since`` onward, synchronously."""
    table = _RUM_FACT_TABLES.get(domain)
    if table is None:
        raise ValueError("domain must be rum_vitals or rum_errors")
    _purge_since(client, table, "event_timestamp", service_id, since)


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
    aggregates_publication: ClickHousePublication | None = None,
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
        aggregates_publication=aggregates_publication,
    )


def purge_cmcd_projections(client: _SqlClient, *, service_id: str, since: datetime) -> None:
    """Delete one service's CMCD projection rows from ``since`` onward, synchronously."""
    _purge_since(client, "cmcd_projection_facts", "event_timestamp", service_id, since)


def _rebuild_request_projections_from_fos(
    catalog: ArchiveManifestCatalog,
    archive: ArchivePublication,
    publication: ClickHousePublication,
    *,
    service_id: str,
    projection_label: str,
    replay_fn: Callable[..., ProjectionReplayResult],
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
    rows = replayed = 0
    errors: list[str] = []
    for manifest in manifests:
        try:
            result = replay_fn(manifest, archive, publication, rebuild_id=resolved_rebuild_id)
        except Exception as exc:
            errors.append(f"manifest {manifest.manifest_id} failed to rebuild {projection_label} projections: {exc}")
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
    return _rebuild_request_projections_from_fos(
        catalog,
        archive,
        publication,
        service_id=service_id,
        projection_label="CMCD",
        replay_fn=replay_cmcd_projections,
        now=now,
        window=window,
        rebuild_id=rebuild_id,
        capacity=capacity,
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
    return _rebuild_request_projections_from_fos(
        catalog,
        archive,
        publication,
        service_id=service_id,
        projection_label="Origin",
        replay_fn=replay_origin_projections,
        now=now,
        window=window,
        rebuild_id=rebuild_id,
        capacity=capacity,
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
    """Populate the Performance waterfall/scatter projections for already-ingested history."""
    return _rebuild_request_projections_from_fos(
        catalog,
        archive,
        publication,
        service_id=service_id,
        projection_label="Performance",
        replay_fn=replay_performance_projections,
        now=now,
        window=window,
        rebuild_id=rebuild_id,
        capacity=capacity,
    )


def replay_security_projections(
    manifest: ArchiveManifest, archive: ArchivePublication, publication: ClickHousePublication, *, rebuild_id: str
) -> ProjectionReplayResult:
    from backend.high_scale.ingest_controller import _enrich_high_scale_ngwaf_bots
    from backend.high_scale.security import build_security_projection_rows

    serving_rows = _load_manifest_request_events(manifest, archive, projection_label="Security")
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
    """Populate ``security_dimensions`` from FOS archive manifests over ``window``."""
    return _rebuild_request_projections_from_fos(
        catalog,
        archive,
        publication,
        service_id=service_id,
        projection_label="Security",
        replay_fn=replay_security_projections,
        now=now,
        window=window,
        rebuild_id=rebuild_id,
        capacity=capacity,
    )


def _sorted(manifests: tuple[ArchiveManifest, ...]) -> tuple[ArchiveManifest, ...]:
    return tuple(sorted(manifests, key=lambda manifest: manifest.coverage_start))


def _replay_derived_projections(
    manifest: ArchiveManifest,
    archive: ArchivePublication,
    publication: ClickHousePublication,
    aggregates_publication: ClickHousePublication,
    rebuild_id: str,
) -> None:
    from backend.high_scale.ingest_controller import _enrich_high_scale_ngwaf_bots
    from backend.high_scale.ingest_controller.service import DIMENSIONS_BY_DOMAIN, compute_dimension_counts
    from backend.high_scale.network import build_network_projection_rows
    from backend.high_scale.origin import build_origin_projection_rows
    from backend.high_scale.performance import build_performance_projection_rows
    from backend.high_scale.security import build_security_projection_rows

    raw_rows = _read_manifest_rows(manifest, archive)
    event_rows = tuple(
        r
        for r in raw_rows
        if r.get("_record_kind") in (None, "event")
        and r.get("timestamp")
        and row_matches_serving_domain(manifest.domain, r)
    )
    if not event_rows:
        return
    if manifest.domain == "request":
        _enrich_high_scale_ngwaf_bots(event_rows)
    if manifest.domain in DIMENSIONS_BY_DOMAIN:
        counts = compute_dimension_counts(manifest.domain, event_rows)
        if counts:
            aggregates_publication.publish(
                _batch_from_rows(
                    f"rebuild:{rebuild_id}:{manifest.manifest_id}:{manifest.domain}_aggregate",
                    manifest,
                    counts,
                    domain=f"{manifest.domain}_aggregate",
                )
            )
    if manifest.domain == "request":
        origin_proj = build_origin_projection_rows(event_rows)
        perf_proj = build_performance_projection_rows(event_rows)
        security_proj = build_security_projection_rows(event_rows)
        network_proj = build_network_projection_rows(event_rows)
        for sub_domain, sub_rows in (
            ("origin_summary", origin_proj.summary_rows),
            ("origin_dimensions", origin_proj.dimension_rows),
            ("performance_dimensions", perf_proj.dimension_rows),
            ("performance_summary", perf_proj.summary_rows),
            ("performance_scatter", perf_proj.scatter_rows),
            ("security_dimensions", security_proj.dimension_rows),
            ("network_dimensions", network_proj.dimension_rows),
        ):
            if sub_rows:
                aggregates_publication.publish(
                    _batch_from_rows(
                        f"rebuild:{rebuild_id}:{manifest.manifest_id}:{sub_domain}",
                        manifest,
                        sub_rows,
                        domain=sub_domain,
                    )
                )
        replay_cmcd_projections(
            manifest,
            archive,
            publication,
            rebuild_id=rebuild_id,
        )


def _replay_all(
    manifests: tuple[ArchiveManifest, ...],
    archive: ArchivePublication,
    publication: ClickHousePublication,
    rebuild_id: str,
    *,
    aggregates_publication: ClickHousePublication | None = None,
) -> tuple[int, int, list[str]]:
    rows = replayed = 0
    errors: list[str] = []
    if aggregates_publication is not None:
        agg_pub = aggregates_publication
    else:
        from backend.high_scale.archive import ClickHouseBatchAdapter
        from backend.high_scale.ingest_controller.service import AggregateBatchAdapter

        client = getattr(publication, "_client", None)
        if isinstance(client, ClickHouseBatchAdapter) and not isinstance(client, AggregateBatchAdapter):
            agg_pub = ClickHousePublication(publication._manifests, AggregateBatchAdapter(client._client))
        else:
            agg_pub = publication
    for manifest in manifests:
        try:
            result = replay_manifest(
                manifest, archive, publication, batch_id=f"rebuild:{rebuild_id}:{manifest.manifest_id}"
            )
            _replay_derived_projections(manifest, archive, publication, agg_pub, rebuild_id)
        except Exception as exc:
            errors.append(f"manifest {manifest.manifest_id} failed to replay: {exc}")
            continue
        rows += result.rows_read
        replayed += 1
    return rows, replayed, errors
