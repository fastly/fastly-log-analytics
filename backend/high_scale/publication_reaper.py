"""Supersede publication manifests orphaned by a failed attempt whose sources were republished elsewhere.

A failed publish leaves its Postgres manifest ``pending`` and returns the sources to ``discovered``. They are
re-claimed in a different page grouping, so the retry has a new archive manifest and a new batch id, and the old
manifest is never retried. This module proves, per pending publication, that its data went out under the
replacement batches, then marks it ``superseded``. It only changes Postgres bookkeeping: ClickHouse rows and
receipts are never touched, so nothing becomes visible or hidden, and ``ClickHousePublication.publish`` refuses a
superseded manifest so a late retry of the old batch can never double count.

Run ``python -m backend.high_scale.publication_reaper --service-id SID`` for a dry run of the whole backlog; add
``--apply`` to write. The worker reaps only orphans younger than ``max_age_seconds`` on its own.

``--abandon-older-than-days N`` also retires pending publications the reaper cannot prove (sources lost, or the
replacement never became visible) once they are older than N days and ClickHouse shows no visible receipt for them.
It marks them ``abandoned``: their rows never became visible and stay invisible, which accepts that data as lost. Only
an operator does this; the worker never abandons.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import uuid
from argparse import ArgumentParser
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from backend.high_scale.archive.clickhouse_batch import publication_batch_uuid
from backend.high_scale.archive.manifests import build_batch_id_from_manifest_id

logger = logging.getLogger("backend.high_scale.publication_reaper")

VisibleReceipts = Callable[[str, frozenset[str]], set[str]]

_TERMINAL_SOURCE_STATUSES = frozenset({"acknowledged", "source_deleted"})


def publication_batch_id(manifest_id: str, manifest_domain: str, domain: str) -> str:
    """The batch id the coordinator gives archive manifest ``manifest_id``'s publication for ``domain``."""
    if domain == manifest_domain:
        return build_batch_id_from_manifest_id(manifest_id)
    return build_batch_id_from_manifest_id(f"{manifest_id}:{domain}")


def clickhouse_visible_receipts(client: Any) -> VisibleReceipts:
    def visible(service_id: str, batch_uuids: frozenset[str]) -> set[str]:
        if not batch_uuids:
            return set()
        literals = ",".join(f"'{uuid.UUID(value)}'" for value in sorted(batch_uuids))
        rows = client.execute(
            "SELECT batch_id FROM high_scale_batch_publications FINAL "
            f"WHERE service_id={{service_id:String}} AND batch_id IN ({literals}) AND publication_state='visible'",
            {"service_id": service_id},
        )
        return {str(row["batch_id"]) for row in rows}

    def visible_with_rows(service_id: str, batch_uuids: frozenset[str]) -> dict[str, int]:
        if not batch_uuids:
            return {}
        literals = ",".join(f"'{uuid.UUID(value)}'" for value in sorted(batch_uuids))
        rows = client.execute(
            "SELECT batch_id, visible_rows FROM high_scale_batch_publications FINAL "
            f"WHERE service_id={{service_id:String}} AND batch_id IN ({literals}) AND publication_state='visible'",
            {"service_id": service_id},
        )
        return {str(row["batch_id"]): int(row.get("visible_rows") or 0) for row in rows}

    visible.with_rows = visible_with_rows  # type: ignore[attr-defined]
    return visible


@dataclass(frozen=True)
class ReapResult:
    superseded: tuple[str, ...]
    kept: dict[str, int]
    applied: int
    scanned: int
    cursor: tuple[datetime, str] | None
    by_domain: dict[str, int] = field(default_factory=dict)
    abandoned: tuple[str, ...] = ()
    abandoned_applied: int = 0
    abandoned_by_domain: dict[str, int] = field(default_factory=dict)
    reconciled: tuple[str, ...] = ()
    reconciled_applied: int = 0
    reconciled_by_domain: dict[str, int] = field(default_factory=dict)


class PublicationReaper:
    def __init__(
        self,
        *,
        control: Any,
        visible_receipts: VisibleReceipts,
        grace_seconds: float = 3600.0,
        max_age_seconds: float | None = None,
        interval_seconds: float = 600.0,
        limit: int = 200,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._control = control
        self._visible_receipts = visible_receipts
        self.grace_seconds = grace_seconds
        self.max_age_seconds = max_age_seconds
        self._interval_seconds = interval_seconds
        self._limit = limit
        self._clock = clock
        self._last_run: dict[str, float] = {}
        self._cursor: dict[str, tuple[datetime, str] | None] = {}

    def maybe_reap(self, service_id: str) -> ReapResult | None:
        now = self._clock()
        last = self._last_run.get(service_id)
        if last is not None and now - last < self._interval_seconds:
            return None
        self._last_run[service_id] = now
        result = self.reap(
            service_id,
            apply=True,
            grace_seconds=self.grace_seconds,
            max_age_seconds=self.max_age_seconds,
            limit=self._limit,
            after=self._cursor.get(service_id),
        )
        self._cursor[service_id] = result.cursor
        if result.applied or result.reconciled_applied or result.kept:
            logger.info(
                "high-scale publication reaper pass",
                extra={
                    "service_id": service_id,
                    "superseded": result.applied,
                    "superseded_by_domain": result.by_domain,
                    "reconciled_visible": result.reconciled_applied,
                    "reconciled_by_domain": result.reconciled_by_domain,
                    "kept": result.kept,
                },
            )
        return result

    def reap(
        self,
        service_id: str,
        *,
        apply: bool,
        grace_seconds: float = 3600.0,
        max_age_seconds: float | None = None,
        limit: int = 500,
        after: tuple[datetime, str] | None = None,
        abandon_after_seconds: float | None = None,
    ) -> ReapResult:
        orphans = self._control.committed_archive_manifests(
            service_id, min_age_seconds=grace_seconds, max_age_seconds=max_age_seconds, after=after, limit=limit
        )
        cursor = (orphans[-1][2], orphans[-1][0]) if len(orphans) >= limit else None
        domains = self._control.pending_publication_domains(service_id)
        if not domains:
            return ReapResult((), {}, 0, len(orphans), cursor)

        candidates = {
            publication_batch_id(manifest_id, manifest_domain, domain): manifest_id
            for manifest_id, manifest_domain, _created in orphans
            for domain in domains
        }
        states = self._control.publication_manifest_states(service_id, tuple(candidates))
        pending: dict[str, dict[str, Any]] = {b: s for b, s in states.items() if s["status"] == "pending"}

        pending_direct = (
            self._control.pending_publication_manifests(service_id, min_age_seconds=grace_seconds, limit=limit)
            if after is None
            else []
        )
        direct_batches: set[str] = set()
        for row in pending_direct:
            b = row["batch_id"]
            direct_batches.add(b)
            if b not in pending:
                pending[b] = {
                    "status": "pending",
                    "domain": row["domain"],
                    "generation": row["generation"],
                    "expected_rows": row["expected_rows"],
                    "age_seconds": row["age_seconds"],
                }

        if not pending:
            return ReapResult((), {}, 0, len(orphans), cursor)

        # Check ClickHouse visible receipts first for every stale pending publication so receipts
        # already visible in ClickHouse are reconciled in Postgres regardless of archive manifest state.
        check_visible = [b for b, s in pending.items() if s["age_seconds"] >= grace_seconds]
        uuids = {
            publication_batch_uuid(service_id, pending[b]["domain"], pending[b]["generation"], b): b
            for b in check_visible
        }
        visible = self._visible_receipts(service_id, frozenset(uuids))
        visible_batches = {uuids[u] for u in visible}
        reconcile_rows_by_batch: dict[str, int] = {}
        if visible:
            with_rows = getattr(self._visible_receipts, "with_rows", None)
            rows_by_uuid = with_rows(service_id, frozenset(visible)) if callable(with_rows) else {}
            for u in visible:
                b = uuids[u]
                reconcile_rows_by_batch[b] = (
                    rows_by_uuid[u] if u in rows_by_uuid else int(pending[b].get("expected_rows") or 0)
                )
        reconciled = tuple(sorted(reconcile_rows_by_batch))
        reconciled_applied = (
            self._control.mark_publications_visible(service_id, reconcile_rows_by_batch)
            if apply and reconcile_rows_by_batch
            else 0
        )

        # Resolve manifest_id for any remaining non-visible pending manifests whose archive row moved
        # out of manifest_committed (only when unbounded by max_age_seconds, or for direct pending rows).
        remaining_pending = [b for b in pending if b not in visible_batches]
        unmatched_pending = {b for b in remaining_pending if b not in candidates}
        if unmatched_pending and max_age_seconds is None:
            unmatched_domains = {pending[b]["domain"] for b in unmatched_pending}
            for manifest_id, manifest_domain in self._control.archive_manifest_domains_for_service(
                service_id, max_age_seconds=max_age_seconds
            ):
                for domain in unmatched_domains:
                    bid = publication_batch_id(manifest_id, manifest_domain, domain)
                    if bid in unmatched_pending and bid not in candidates:
                        candidates[bid] = manifest_id

        kept: Counter[str] = Counter()
        unprovable: list[str] = []
        provable_pending = [b for b in remaining_pending if b in candidates]
        sources = self._control.archive_manifest_source_states(
            service_id, sorted({candidates[b] for b in provable_pending})
        )
        replacements: dict[str, frozenset[str]] = {}
        for batch_id in provable_pending:
            manifest_id = candidates[batch_id]
            owners = _republished_under(manifest_id, sources.get(manifest_id, ()))
            if owners is None:
                kept["sources_not_republished"] += 1
                unprovable.append(batch_id)
            elif pending[batch_id]["age_seconds"] < grace_seconds:
                kept["recently_updated"] += 1
            else:
                replacements[batch_id] = owners
        for batch_id in remaining_pending:
            if batch_id not in candidates and batch_id in direct_batches:
                kept["sources_not_republished"] += 1
                unprovable.append(batch_id)

        replacement_domains = self._control.archive_manifest_domains(
            service_id, sorted({m for owners in replacements.values() for m in owners})
        )
        replacement_batches = {
            batch_id: {
                publication_batch_id(m, replacement_domains[m], pending[batch_id]["domain"])
                for m in owners
                if m in replacement_domains
            }
            for batch_id, owners in replacements.items()
        }
        replacement_states = self._control.publication_manifest_states(
            service_id, sorted({b for batches in replacement_batches.values() for b in batches})
        )
        proven: list[str] = []
        for batch_id, batches in replacement_batches.items():
            statuses = [replacement_states[b]["status"] for b in batches if b in replacement_states]
            # A replacement with no publication for this domain had no rows for it; one that is not visible
            # (or a replacement manifest that cannot be found) leaves the data unproven.
            if len(batches) != len(replacements[batch_id]) or not statuses or set(statuses) != {"visible"}:
                kept["replacement_not_visible"] += 1
                unprovable.append(batch_id)
            else:
                proven.append(batch_id)

        stale = (
            [b for b in unprovable if pending[b]["age_seconds"] >= abandon_after_seconds]
            if abandon_after_seconds is not None
            else []
        )
        superseded = tuple(sorted(proven))
        abandoned = tuple(sorted(stale))
        applied = self._control.mark_publications_superseded(service_id, superseded) if apply and superseded else 0
        abandoned_applied = (
            self._control.mark_publications_abandoned(service_id, abandoned) if apply and abandoned else 0
        )
        return ReapResult(
            superseded,
            dict(kept),
            applied,
            len(orphans),
            cursor,
            dict(sorted(Counter(pending[b]["domain"] for b in superseded).items())),
            abandoned,
            abandoned_applied,
            dict(sorted(Counter(pending[b]["domain"] for b in abandoned).items())),
            reconciled,
            reconciled_applied,
            dict(sorted(Counter(pending[b]["domain"] for b in reconciled).items())),
        )


def _republished_under(manifest_id: str, sources: Sequence[tuple[str | None, str | None]]) -> frozenset[str] | None:
    """The archive manifests that acknowledged every one of this manifest's sources, or None if any did not."""
    if not sources:
        return None
    owners = set()
    for status, owner in sources:
        if status not in _TERMINAL_SOURCE_STATUSES or not owner or owner == manifest_id:
            return None
        owners.add(owner)
    return frozenset(owners)


def _visible_receipts_from_environment() -> VisibleReceipts:
    from backend import config as app_config
    from backend.core.clickhouse import ClickHouseClient

    settings = app_config.load_clickhouse_config()
    if settings is None:
        raise RuntimeError("publication reaper requires CLICKHOUSE_ENABLED=true")
    return clickhouse_visible_receipts(ClickHouseClient(settings))


def reaper_from_environment(control: Any, clickhouse: Any) -> PublicationReaper | None:
    if os.getenv("HIGH_SCALE_PUBLICATION_REAPER_ENABLED", "1").strip().lower() not in {"1", "true", "yes", "on"}:
        return None
    raw_max_age = os.getenv("HIGH_SCALE_PUBLICATION_REAPER_MAX_AGE_SECONDS", "").strip()
    return PublicationReaper(
        control=control,
        visible_receipts=clickhouse_visible_receipts(clickhouse),
        grace_seconds=float(os.getenv("HIGH_SCALE_PUBLICATION_REAPER_GRACE_SECONDS", "300")),
        max_age_seconds=float(raw_max_age) if raw_max_age else None,
        interval_seconds=float(os.getenv("HIGH_SCALE_PUBLICATION_REAPER_INTERVAL_SECONDS", "60")),
    )


def main(argv: Sequence[str] | None = None) -> int:
    from backend.core.metadata.base import closing_pg_pool

    with closing_pg_pool():
        return _main(argv)


def _main(argv: Sequence[str] | None) -> int:
    from backend.high_scale.postgres_control import PostgresControlPlane

    parser = ArgumentParser(description="Supersede orphaned high-scale publication manifests (dry run by default)")
    parser.add_argument("--service-id", required=True)
    parser.add_argument("--apply", action="store_true", help="write the superseded status (default: dry run)")
    parser.add_argument("--grace-seconds", type=float, default=3600.0)
    parser.add_argument("--max-age-seconds", type=float, default=None)
    parser.add_argument("--page-size", type=int, default=500)
    parser.add_argument("--max-pages", type=int, default=1000)
    parser.add_argument(
        "--abandon-older-than-days",
        type=float,
        default=None,
        help="also mark unprovable pending publications older than this abandoned (accepts their data as lost)",
    )
    args = parser.parse_args(argv)
    if args.abandon_older_than_days is not None and args.abandon_older_than_days <= 0:
        parser.error("--abandon-older-than-days must be positive")
    abandon_after = None if args.abandon_older_than_days is None else args.abandon_older_than_days * 86400

    reaper = PublicationReaper(control=PostgresControlPlane(), visible_receipts=_visible_receipts_from_environment())
    by_domain: Counter[str] = Counter()
    abandoned_by_domain: Counter[str] = Counter()
    kept: Counter[str] = Counter()
    applied = abandoned = scanned = pages = 0
    cursor: tuple[datetime, str] | None = None
    while pages < args.max_pages:
        result = reaper.reap(
            args.service_id,
            apply=args.apply,
            grace_seconds=args.grace_seconds,
            max_age_seconds=args.max_age_seconds,
            limit=args.page_size,
            after=cursor,
            abandon_after_seconds=abandon_after,
        )
        pages += 1
        by_domain.update(result.by_domain)
        abandoned_by_domain.update(result.abandoned_by_domain)
        abandoned += result.abandoned_applied
        kept.update(result.kept)
        applied += result.applied
        scanned += result.scanned
        cursor = result.cursor
        if cursor is None:
            break
    sys.stdout.write(
        json.dumps(
            {
                "service_id": args.service_id,
                "apply": args.apply,
                "scanned_archive_manifests": scanned,
                "would_supersede": dict(sorted(by_domain.items())),
                "applied": applied,
                "would_abandon": dict(sorted(abandoned_by_domain.items())),
                "abandoned": abandoned,
                "kept": dict(sorted(kept.items())),
                "complete": cursor is None,
            },
            indent=1,
        )
        + "\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
