"""Bounded, read-only planning for exact retained RUM identities.

Unlike disaster rebuilds this never assumes an empty destination. The caller
must fence terminal sources and serialize apply, re-plan under that fence,
publish only these batches, then verify a zero-missing plan.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from backend.high_scale.archive_models import ArchiveManifest, ArchiveSourceObject
from backend.high_scale.archive_publication import ArchivePublication
from backend.high_scale.decoder import decode_source_object
from backend.high_scale.publication import (
    _DOMAIN_COLUMNS,
    _DOMAIN_TABLES,
    ClickHouseBatchAdapter,
    ClickHousePublication,
    HighScaleBatch,
    PartialInsert,
    _batch_uuid,
    _rows_for_domain,
    row_matches_serving_domain,
)
from backend.high_scale.replay import _read_manifest_rows

_DOMAINS = ("rum_vitals", "rum_errors")
_IGNORED_CONTENT = {"event_id", "transform_version", "ingest_timestamp", "batch_id", "publication_state"}
_READ_LIMITS = {
    "max_rows_to_read": "100000",
    "read_overflow_mode": "throw",
    "max_result_rows": "1001",
    "result_overflow_mode": "throw",
}


class RumRecoveryBatchAdapter(ClickHouseBatchAdapter):
    """Recovery-only GLOBAL batch verification without one unbounded scan.

    Every active partition is covered, including dates outside the candidate
    window. Metadata weights only group reads; they never establish absence.
    All physical target rows are read and compared, not just counted. Any
    incomplete inventory, changing partition set, cap failure or partial/
    conflicting payload aborts. No delete-and-retry path is taken.
    """

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
            placeholders = ",".join("{part" + str(index) + ":String}" for index in range(len(keys)))
            rows = self._client.execute(
                f"SELECT {','.join(_DOMAIN_COLUMNS[batch.domain])} FROM {table} "
                "WHERE service_id={service_id:String} AND batch_id={batch_id:UUID} "
                "AND _partition_id IN (" + placeholders + ") LIMIT 1001",
                {
                    "service_id": batch.service_id,
                    "batch_id": batch_uuid,
                    **{f"part{index}": key for index, key in enumerate(keys)},
                },
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
        # Validate physical rows even on the parent's visible-receipt fast path.
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
            # Float64 JSON is allowed to emit integral values as integers.
            # Normalize representation, never round away numeric differences.
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
    """Reconstruct each original source line once, preserving ordinals/version.

    ``read_facts`` must read physical rows across ALL batch IDs for exactly the
    service/sources supplied, including unpublished rows. It supplies
    ``repair_visible`` from the domain/service publication fence, not merely the
    fact row's own publication_state. Hidden, duplicate or conflicting rows
    refuse the whole plan. Identical content under an older transform also
    counts as existing; changing event IDs is not permission to duplicate it.
    """
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
            ordinal = row.get("line_ordinal")
            raw = row.get("raw_json")
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
            for event in decoded.events:
                if row_matches_serving_domain(domain, event):
                    candidates[domain].append(event)
    if covered_offsets != set(range(len(rows))):
        raise ValueError("RUM repair source slices do not cover archive")
    if quarantined:
        # No partial apply when a changed parser cannot reconstruct a source.
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
        for fact in facts:
            timestamp = fact["event_timestamp"]
            if timestamp < observed - timedelta(days=30) or timestamp > observed:
                raise ValueError("RUM repair event is outside retained fact window")
        expected.extend(facts)
        present = [
            row
            for row in read_facts(domain, tuple(s.key for s in sources))
            if row.get("_repair_domain", domain) == domain
        ]
        if len(present) > max_rows:
            raise ValueError("RUM repair existing facts exceed row bound")
        present_ids = [str(row.get("event_id")) for row in present]
        if len(present_ids) != len(set(present_ids)):
            raise ValueError("RUM repair existing identity is ambiguous or unpublished")
        missing: list[dict[str, Any]] = []
        for event, fact in zip(events, facts, strict=True):
            matches = [
                row
                for row in present
                if str(row.get("event_id")) == fact["event_id"] or _content(row, domain) == _content(fact, domain)
            ]
            if not matches:
                missing.append(event)
            elif (
                len(matches) != 1
                or not matches[0].get("repair_visible")
                or _content(matches[0], domain) != _content(fact, domain)
            ):
                raise ValueError("RUM repair existing identity is ambiguous or unpublished")
            elif str(matches[0]["event_id"]) == fact["event_id"]:
                exact_counts[domain] += 1
            else:
                equivalent_counts[domain] += 1
        missing_counts[domain] = len(missing)
        if missing:
            # The exact missing identity set defines the batch, never a random
            # rebuild id or an expiring ClickHouse block-dedup token.
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
