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

from backend.high_scale.archive_models import ArchiveManifest
from backend.high_scale.archive_publication import ArchivePublication
from backend.high_scale.publication import ClickHousePublication
from backend.high_scale.replay import replay_manifest, replay_origin_projections


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
) -> RecoveryReport:
    if window <= timedelta(0):
        raise ValueError("window must be positive")
    observed = (now or datetime.now(UTC)).astimezone(UTC)
    resolved_rebuild_id = rebuild_id or uuid4().hex
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
