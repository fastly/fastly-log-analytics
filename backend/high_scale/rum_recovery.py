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
    ClickHousePublication,
    HighScaleBatch,
    _rows_for_domain,
    row_matches_serving_domain,
)
from backend.high_scale.replay import _read_manifest_rows

_DOMAINS = ("rum_vitals", "rum_errors")
_IGNORED_CONTENT = {"event_id", "transform_version", "ingest_timestamp", "batch_id", "publication_state"}


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
