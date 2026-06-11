"""Data-migration framework for per-service one-time setup tasks.

Background — why a second migration system?
    ``backend.core.sqlite_migrations`` already exists for SCHEMA changes
    (CREATE TABLE / ADD COLUMN) on the per-service metadata.db. Those run
    synchronously inside ``_init_schema``, must be transactional, and are
    cheap — a fresh DB has the latest ``_SCHEMA`` and migrations are
    no-ops on it.

    Data migrations are different: long-running, non-transactional setup
    work that touches state OUTSIDE the metadata.db (e.g. the rollups
    parquet files under ``<cache>/rollups/``). The rollups initial
    backfill on a service with months of data can take many minutes; we
    cannot block FastAPI startup behind it (containerised deploys kill
    the boot loop on healthcheck timeout).

Design:
    * ``MIGRATIONS: list[Migration]`` — ordered registry, append-only. The
      list order IS the run order.
    * A row in the per-service ``applied_data_migrations`` table marks a
      migration as done. Failed migrations leave NO row and retry on the
      next boot.
    * ``run_pending(service_id, source)`` diffs the registry against the
      table, spawns ONE daemon thread per service to run the unapplied
      migrations in sequence. Across services they parallelise.
    * Each migration is a pure function ``(service_id, source) -> str | None``.
      The return string is recorded in the ``notes`` column for audit.
      Exceptions bubble up to the runner, which logs + skips the row write.

Adding a migration:
    1. Write an idempotent function ``def _migrate_<short_name>(service_id,
       source) -> str | None:`` somewhere appropriate (typically in the
       module that owns the affected data — e.g. rollups migration lives
       in ``backend.core.rollups``).
    2. Append ``Migration(...)`` to ``MIGRATIONS`` below with a stable
       date-prefixed name (``"YYYY-MM-DD_short_description"``).
    3. The next service-boot picks it up automatically. No manual run-
       once script needed.

What this is NOT:
    * Not a schema migration tool — use ``sqlite_migrations.py`` for DDL.
    * Not a transactional system — individual migrations should write
      their own progress markers (per-field stamps, etc.) so a crash
      mid-run can be detected and partial work resumed on next attempt.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Migration:
    name: str
    description: str
    fn: Callable[[str, dict], str | None]


def _rollups_initial_backfill(service_id: str, source: dict) -> str | None:
    """Build the initial hourly top-N rollups for the dashboard.

    Idempotent: ``ensure_field_backfills`` checks per-field markers in
    ``<cache>/rollups/backfill_markers.json`` and only re-runs the COPY
    for fields without a marker. Safe to retry after a crash.
    """
    from backend.core import rollups

    rollups.ensure_field_backfills(service_id, source)
    return "rollups: ensure_field_backfills complete"


def _rollups_hour_bundling_backfill(service_id: str, source: dict) -> str | None:
    """Bundle all closed-hour per-field rollup parquets into a single
    per-hour parquet under ``rollups/hour_bundled/hour=H/all_fields.parquet``.

    The dashboard reader prefers bundled files (one open per hour) over
    per-field files (~40 opens per hour), cutting cold-path parquet
    metadata reads by ~40x on a 24h query. The per-field tree stays in
    place — the reader falls back to it when a bundle is missing, so the
    migration is non-destructive.

    Idempotent: bundle_hours skips hours whose bundle is already up to
    date (mtime check), so re-running is cheap.
    """
    from backend.core import rollups

    n = rollups.backfill_hour_bundles(service_id, source)
    return f"rollups: bundled {n} hour(s) into hour_bundled/"


def _rollups_time_series_backfill(service_id: str, source: dict) -> str | None:
    """Build the per-hour 1-minute time_series.parquet bundles for every
    closed hour that doesn't yet have one.

    The dashboard chart's rollup fast-path
    (``QueryRunner.try_time_series_from_rollup``) requires a
    ``time_series.parquet`` for EVERY closed hour in the requested
    window — one missing hour disqualifies the whole range and falls
    back to a raw Iceberg scan that costs ~16-19 s for 30d.

    The writer ``build_time_series_bundles`` only ever runs against
    hours TOUCHED by the most recent sync tick, so services that
    pre-date the time_series feature (added late) have a giant gap of
    historical hours with no time_series.parquet. This migration
    closes the gap once per service.

    Idempotent: ``backfill_time_series_bundles`` only builds for hours
    that don't already have a ``time_series.parquet``, so re-running
    after a partial failure is cheap.
    """
    from backend.core import rollups

    n = rollups.backfill_time_series_bundles(service_id, source)
    return f"rollups: built time_series.parquet for {n} historical hour(s)"


# Ordered registry. Append-only — never remove or reorder entries.
# Names must be globally unique and stable; the DB matches by name.
MIGRATIONS: list[Migration] = [
    Migration(
        name="2026-06-04_rollups_initial_backfill",
        description="Build initial hourly top-N rollups for dashboard top-N queries",
        fn=_rollups_initial_backfill,
    ),
    Migration(
        name="2026-06-08_rollups_hour_bundling",
        description="Bundle per-field hour rollups into one parquet per hour (40x fewer file opens)",
        fn=_rollups_hour_bundling_backfill,
    ),
    Migration(
        name="2026-06-10_rollups_time_series_backfill",
        description="Backfill time_series.parquet for all closed hours so the dashboard chart's rollup fast-path covers 7d/30d",
        fn=_rollups_time_series_backfill,
    ),
]


def list_pending(service_id: str) -> list[Migration]:
    """Return registered migrations that haven't been applied to this service."""
    from backend.core import metadata_db

    applied = metadata_db.list_applied_data_migrations(service_id)
    return [m for m in MIGRATIONS if m.name not in applied]


def run_pending(service_id: str, source: dict) -> None:
    """Spawn a daemon thread that runs pending data migrations sequentially.

    Returns immediately — does not block the caller. Per-service threads
    are independent, so several services with pending migrations apply
    in parallel; within a single service the migrations run in registry
    order.
    """
    pending = list_pending(service_id)
    if not pending:
        return
    names = [m.name for m in pending]
    logger.info("[migrations] service %s: %d pending — %s", service_id, len(pending), names)
    t = threading.Thread(
        target=_run_sequence,
        args=(service_id, source, pending),
        daemon=True,
        name=f"data-migrations-{service_id}",
    )
    t.start()


def _run_sequence(service_id: str, source: dict, migrations: list[Migration]) -> None:
    from backend.core import metadata_db

    for mig in migrations:
        t0 = time.time()
        logger.info("[migrations] %s/%s: starting — %s", service_id, mig.name, mig.description)
        try:
            notes = mig.fn(service_id, source)
        except Exception as e:
            logger.exception(
                "[migrations] %s/%s: FAILED after %.2fs — will retry next startup: %s",
                service_id,
                mig.name,
                time.time() - t0,
                e,
            )
            # Important: do NOT record this migration as applied. Returning
            # here also halts the sequence — a later migration that depends
            # on a failed predecessor must not be allowed to run.
            return
        duration = time.time() - t0
        try:
            metadata_db.record_applied_data_migration(
                service_id, mig.name, duration_s=duration, status="success", notes=notes
            )
        except Exception as e:
            # Recording failed but the migration itself succeeded. Next boot
            # will re-run it; the migration is idempotent so this is safe,
            # just wasted work. Loud warning so we can spot the divergence.
            logger.warning(
                "[migrations] %s/%s: applied but COULD NOT RECORD (will re-run next boot): %s",
                service_id,
                mig.name,
                e,
            )
            continue
        logger.info("[migrations] %s/%s: applied in %.2fs", service_id, mig.name, duration)
