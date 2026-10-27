"""FOS-authoritative recovery and bounded RUM recovery planning & execution for high-scale."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol
from uuid import uuid4

import structlog

from backend.high_scale._common import ArchiveManifestCatalog
from backend.high_scale.archive import (
    _DOMAIN_COLUMNS,
    _DOMAIN_TABLES,
    ArchiveManifest,
    ArchivePublication,
    ArchiveSourceObject,
    ClickHouseBatchAdapter,
    ClickHousePublication,
    HighScaleBatch,
    PartialInsert,
    ProjectionReplayResult,
    _batch_from_rows,
    _batch_uuid,
    _load_manifest_request_events,
    _read_manifest_rows,
    _rows_for_domain,
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
        recent_manifests, archive, publication, resolved_rebuild_id
    )
    warm_rows, warm_replayed, warm_errors = _replay_all(warm_manifests, archive, publication, resolved_rebuild_id)
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


def _replay_all(
    manifests: tuple[ArchiveManifest, ...],
    archive: ArchivePublication,
    publication: ClickHousePublication,
    rebuild_id: str,
) -> tuple[int, int, list[str]]:
    rows = replayed = 0
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


# ── Bounded RUM Recovery Planning & Execution ─────────────────────────────────
_DOMAINS = ("rum_vitals", "rum_errors")
_IGNORED_CONTENT = {"event_id", "transform_version", "ingest_timestamp", "batch_id", "publication_state"}
_READ_LIMITS = {
    "max_rows_to_read": "100000",
    "read_overflow_mode": "throw",
    "max_result_rows": "1001",
    "result_overflow_mode": "throw",
}


class RumRecoveryBatchAdapter(ClickHouseBatchAdapter):
    """Recovery-only GLOBAL batch verification without one unbounded scan."""

    def _inventory(self, table: str) -> dict[str, int]:
        rows = self._client.execute(
            "SELECT partition_id,sum(rows) AS partition_rows FROM system.parts "
            "WHERE database=currentDatabase() AND table={table:String} AND active "
            "GROUP BY partition_id LIMIT 33",
            {"table": table},
            settings=_READ_LIMITS,
        )
        if len(rows) > 32:
            raise ValueError("RUM repair global partition inventory exceeds bound")
        result: dict[str, int] = {}
        for row in rows:
            key, weight = row["partition_id"], row["partition_rows"]
            if (
                not isinstance(key, str)
                or not key
                or key in result
                or not isinstance(weight, int)
                or isinstance(weight, bool)
                or not 0 <= weight <= 90_000
            ):
                raise ValueError("RUM repair partition cannot be proved under read cap")
            result[key] = weight
        return result

    def _count_rows(self, table: str, batch: HighScaleBatch, batch_uuid: str) -> int:
        if batch.domain not in _DOMAINS or table != _DOMAIN_TABLES[batch.domain] or not 0 < len(batch.rows) <= 1000:
            raise ValueError("RUM repair batch verification scope is invalid")
        inventory = self._inventory(table)
        groups: list[tuple[list[str], int]] = []
        for key, weight in sorted(inventory.items(), key=lambda item: (-item[1], item[0])):
            for index, (keys, total) in enumerate(groups):
                if total + weight <= 90_000:
                    keys.append(key)
                    groups[index] = (keys, total + weight)
                    break
            else:
                groups.append(([key], weight))
        if len(groups) > 6:
            raise ValueError("RUM repair global partition read groups exceed bound")
        expected = [
            dict(zip(_DOMAIN_COLUMNS[batch.domain], values, strict=True))
            for values in _rows_for_domain(batch, batch_uuid)
        ]
        expected_by_id = {str(row["event_id"]): row for row in expected}
        if len(expected_by_id) != len(expected):
            raise ValueError("RUM repair batch candidate identities are ambiguous")
        present: list[dict[str, Any]] = []
        for keys, _ in groups:
            placeholders = ",".join(f"{{part{i}:String}}" for i in range(len(keys)))
            part_params = {f"part{i}": k for i, k in enumerate(keys)}
            rows = self._client.execute(
                f"SELECT {','.join(_DOMAIN_COLUMNS[batch.domain])} FROM {table} "
                f"WHERE service_id={{service_id:String}} AND batch_id={{batch_id:UUID}} "
                f"AND _partition_id IN ({placeholders}) LIMIT 1001",
                {"service_id": batch.service_id, "batch_id": batch_uuid, **part_params},
                settings=_READ_LIMITS,
            )
            present.extend(rows)
            if len(present) > 1000:
                raise ValueError("RUM repair global batch facts exceed row bound")
        if set(self._inventory(table)) != set(inventory):
            raise ValueError("RUM repair global partition inventory changed")
        seen: set[str] = set()
        for row in present:
            identity = str(row["event_id"])
            wanted = expected_by_id.get(identity)
            if (
                identity in seen
                or wanted is None
                or str(row["batch_id"]) != batch_uuid
                or _content(row, batch.domain) != _content(wanted, batch.domain)
                or row["transform_version"] != wanted["transform_version"]
                or row["publication_state"] != "visible"
            ):
                raise ValueError("RUM repair existing batch facts are duplicate, conflicting or out of scope")
            seen.add(identity)
        if len(present) not in (0, len(expected)):
            raise PartialInsert(len(present))
        return len(present)

    def inspect_batch(self, batch: HighScaleBatch) -> tuple[int, dict[str, Any] | None]:
        """Read-only preflight: exact global physical proof plus full-key receipt."""
        batch_uuid = _batch_uuid(batch)
        count = self._count_rows(_DOMAIN_TABLES[batch.domain], batch, batch_uuid)
        existing = super()._existing_publication(batch, batch_uuid)
        if existing is not None:
            self._validate_publication(existing, batch)
            if existing["publication_state"] == "visible" and count != len(batch.rows):
                raise PartialInsert(count)
        return count, existing

    def _existing_publication(self, batch: HighScaleBatch, batch_uuid: str) -> dict[str, Any] | None:
        return self.inspect_batch(batch)[1]


@dataclass(frozen=True)
class RumRecoveryPlan:
    plan_digest: str
    candidate_counts: dict[str, int]
    missing_counts: dict[str, int]
    exact_identity_counts: dict[str, int]
    equivalent_identity_counts: dict[str, int]
    batches: tuple[HighScaleBatch, ...]
    expected_facts: tuple[dict[str, Any], ...]
    source_count: int
    raw_line_count: int
    quarantined_lines: int

    def summary(self) -> dict[str, Any]:
        """Allowlisted operational evidence: no service/source keys or beacons."""
        return {
            "plan_digest": self.plan_digest,
            "candidate_counts": self.candidate_counts,
            "missing_counts": self.missing_counts,
            "exact_identity_counts": self.exact_identity_counts,
            "equivalent_identity_counts": self.equivalent_identity_counts,
            "batches": len(self.batches),
            "sources": self.source_count,
            "raw_lines": self.raw_line_count,
            "quarantined_lines": self.quarantined_lines,
        }


def _canonical(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat(timespec="milliseconds")
    return value


def _content(row: Mapping[str, Any], domain: str) -> str:
    values = {}
    for name in _DOMAIN_COLUMNS[domain]:
        if name in _IGNORED_CONTENT:
            continue
        value = row.get(name)
        if name == "event_timestamp" and isinstance(value, str):
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
            value = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
        if name in {"metric_value", "ttfb"} and isinstance(value, (int, float)) and not isinstance(value, bool):
            value = float(value)
        values[name] = _canonical(value)
    return json.dumps(values, sort_keys=True, separators=(",", ":"))


def plan_rum_recovery(
    manifest: ArchiveManifest,
    archive: ArchivePublication,
    *,
    owner_epoch: int,
    read_facts: Callable[[str, tuple[str, ...]], Sequence[Mapping[str, Any]]],
    now: datetime | None = None,
    max_rows: int = 1000,
) -> RumRecoveryPlan:
    """Reconstruct each original source line once, preserving ordinals/version."""
    from backend.high_scale.query import decode_source_object

    if manifest.domain not in _DOMAINS or owner_epoch < 1 or not 0 < max_rows <= 1000:
        raise ValueError("invalid bounded RUM repair scope")
    sources = manifest.sources
    if not sources or len(sources) > 100 or len({s.key for s in sources}) != len(sources):
        raise ValueError("RUM repair requires unique bounded manifest sources")
    rows = _read_manifest_rows(manifest, archive)
    if len(rows) > max_rows:
        raise ValueError("RUM repair archive exceeds row bound")
    observed = (now or datetime.now(UTC)).astimezone(UTC)
    candidates: dict[str, list[dict[str, Any]]] = {domain: [] for domain in _DOMAINS}
    raw_lines = 0
    quarantined: set[tuple[str, int]] = set()
    covered_offsets: set[int] = set()
    for source in sources:
        end = source.row_start + source.row_count
        if source.row_start < 0 or end > len(rows):
            raise ValueError("RUM repair source slice is invalid")
        offsets = set(range(source.row_start, end))
        if covered_offsets & offsets:
            raise ValueError("RUM repair source slices overlap")
        covered_offsets.update(offsets)
        lines: dict[int, str] = {}
        version = source.version or source.checksum
        for row in rows[source.row_start : end]:
            if row.get("_record_kind", "event") != "event":
                raise ValueError("RUM repair requires reviewed event-only source slices")
            ordinal, raw = row.get("line_ordinal"), row.get("raw_json")
            if (
                row.get("service_id") != manifest.service_id
                or row.get("source_object_key") != source.key
                or row.get("source_object_version") != version
                or row.get("transform_version") != manifest.artifact.transform_version
                or not isinstance(ordinal, int)
                or not 0 <= ordinal < 10000
                or not isinstance(raw, str)
                or "\n" in raw
                or "\r" in raw
            ):
                raise ValueError("RUM repair source provenance is invalid")
            if ordinal in lines and lines[ordinal] != raw:
                raise ValueError("RUM repair source line is ambiguous")
            lines[ordinal] = raw
        raw_lines += len(lines)
        if not lines:
            continue
        payload = "\n".join(lines.get(i, "") for i in range(max(lines) + 1)).encode()
        descriptor = ArchiveSourceObject(
            manifest.service_id, manifest.domain, source.key, source.checksum, source.size_bytes, source.version
        )
        for domain in _DOMAINS:
            decoded = decode_source_object(
                descriptor, payload, domain=domain, transform_version=manifest.artifact.transform_version
            )
            quarantined.update((source.key, item.line_ordinal) for item in decoded.dead_letters)
            candidates[domain].extend(e for e in decoded.events if row_matches_serving_domain(domain, e))
    if covered_offsets != set(range(len(rows))):
        raise ValueError("RUM repair source slices do not cover archive")
    if quarantined:
        raise ValueError("RUM repair found quarantined source lines; review exact archive separately")
    if sum(map(len, candidates.values())) > max_rows:
        raise ValueError("RUM repair expansion exceeds row bound")

    expected: list[dict[str, Any]] = []
    batches: list[HighScaleBatch] = []
    missing_counts: dict[str, int] = {}
    exact_counts = {domain: 0 for domain in _DOMAINS}
    equivalent_counts = {domain: 0 for domain in _DOMAINS}
    for domain in _DOMAINS:
        events = candidates[domain]
        batch = HighScaleBatch("plan", manifest.service_id, domain, str(owner_epoch), tuple(events))
        mapped = _rows_for_domain(batch, "00000000-0000-0000-0000-000000000000")
        facts = [dict(zip(_DOMAIN_COLUMNS[domain], row, strict=True), _repair_domain=domain) for row in mapped]
        ids = [fact["event_id"] for fact in facts]
        if len(ids) != len(set(ids)):
            raise ValueError("RUM repair candidate identities are ambiguous")
        if any(f["event_timestamp"] < observed - timedelta(days=30) or f["event_timestamp"] > observed for f in facts):
            raise ValueError("RUM repair event is outside retained fact window")
        expected.extend(facts)
        present = [
            r for r in read_facts(domain, tuple(s.key for s in sources)) if r.get("_repair_domain", domain) == domain
        ]
        if len(present) > max_rows:
            raise ValueError("RUM repair existing facts exceed row bound")
        present_ids = [str(row.get("event_id")) for row in present]
        if len(present_ids) != len(set(present_ids)):
            raise ValueError("RUM repair existing identity is ambiguous or unpublished")
        missing: list[dict[str, Any]] = []
        for event, fact in zip(events, facts, strict=True):
            fact_content = _content(fact, domain)
            matches = [
                r for r in present if str(r.get("event_id")) == fact["event_id"] or _content(r, domain) == fact_content
            ]
            if not matches:
                missing.append(event)
            elif (
                len(matches) != 1
                or not matches[0].get("repair_visible")
                or _content(matches[0], domain) != fact_content
            ):
                raise ValueError("RUM repair existing identity is ambiguous or unpublished")
            elif str(matches[0]["event_id"]) == fact["event_id"]:
                exact_counts[domain] += 1
            else:
                equivalent_counts[domain] += 1
        missing_counts[domain] = len(missing)
        if missing:
            token = hashlib.sha256("|".join(sorted(event["event_id"] for event in missing)).encode()).hexdigest()
            batches.append(
                HighScaleBatch(
                    f"rum-repair-v1:{manifest.manifest_id}:{domain}:{token}",
                    manifest.service_id,
                    domain,
                    str(owner_epoch),
                    tuple(missing),
                )
            )
    approval = {
        "version": "rum-repair-v1",
        "manifest": manifest.manifest_id,
        "artifact": manifest.artifact.checksum,
        "owner_epoch": owner_epoch,
        "facts": [
            {
                **fact,
                "event_timestamp": _canonical(fact["event_timestamp"]),
                "ingest_timestamp": _canonical(fact["ingest_timestamp"]),
            }
            for fact in expected
        ],
    }
    digest = hashlib.sha256(json.dumps(approval, sort_keys=True, default=str).encode()).hexdigest()
    return RumRecoveryPlan(
        digest,
        {domain: len(candidates[domain]) for domain in _DOMAINS},
        missing_counts,
        exact_counts,
        equivalent_counts,
        tuple(batches),
        tuple(expected),
        len(sources),
        raw_lines,
        len(quarantined),
    )


def execute_rum_recovery(
    make_plan: Callable[[], RumRecoveryPlan],
    publication: ClickHousePublication,
    *,
    apply: bool = False,
    approve_digest: str | None = None,
) -> dict[str, Any]:
    """Caller holds the terminal-source/owner fence for this entire operation."""
    plan = make_plan()
    report = {"applied": False, **plan.summary()}
    if not apply:
        return report
    if approve_digest != plan.plan_digest:
        raise ValueError("RUM repair requires the exact reviewed plan digest")
    for batch in plan.batches:
        publication.publish(batch)
    verified = make_plan()
    if verified.plan_digest != plan.plan_digest or any(verified.missing_counts.values()):
        raise ValueError("RUM repair post-apply identity verification failed")
    return {**report, "applied": True, "remaining_counts": verified.missing_counts}
