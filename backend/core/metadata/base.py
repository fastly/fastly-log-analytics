"""Shared connection management + schema for the per-service metadata store.

Postgres (via ``METADATA_DSN``) is the only connection backend: every
concern-specific module in ``backend.core.metadata`` (alerts, views,
ingest_log, cron_log, asn_cache, usage_log, reconciliation, state) imports
``get_con`` from here and writes through it, and every call lands on the
shared Postgres pool in :mod:`backend.core.metadata.pg_connection`. Schema
bootstrap for that pool goes through
:func:`backend.core.metadata.pg_schema.ensure_pg_schema`, wired at API/Celery
process startup — NOT through this module, which only holds ``_SCHEMA`` (the
schema-of-record, translated to Postgres DDL by ``pg_schema._to_postgres``)
and the dedup filename cache.

Carved out of the historical ``backend.core.metadata`` monolith — callers
should import from ``backend.core.metadata`` (or its concern-specific
submodules). The package's ``__init__`` installs a ``_ShimModule`` proxy that
mirrors writes to shared-state attributes here, so monkeypatch sites in tests
work without addressing the submodule directly.
"""

from __future__ import annotations

import logging
import os
import re
import threading

from backend.core.metadata import pg_connection

logger = logging.getLogger(__name__)

# ``_DATA_DIR`` is retained (rather than deleted outright) because
# ``backend.core.metadata.usage_log_db.db_path`` imports it directly from this
# module, and ``db_path()`` (below) is still called by
# ``alerts.list_alerts_cross_service``, ``reconciliation`` and
# ``repositories._base.attach_metadata_db``, which build the legacy per-service
# SQLite path for their own purposes. Nothing in THIS module's connection path
# (``get_con`` et al., all Postgres-only below) reads it.
_DATA_DIR = "data/services"

# Process-wide cache of {service_id: set[file_name]} for ingest dedup.
# ``get_ingested_filenames`` populates lazily on the first bounded read
# (cron hot path passes ``limit=200_000``); ``insert_ingested_files`` keeps
# it in sync. Unbounded reads (admin teardown / repair tools) bypass and
# invalidate the cache. Eliminates the ~640 ms SQL fetchall on every ~5 s
# sync tick for services with >1 M ingested_files.
_ingested_filenames_cache: dict[str | tuple[str, str], set[str]] = {}
_ingested_filenames_cache_lock = threading.Lock()


# Pre-compiled for the per-insert file_date parse. The canonical Fastly
# basename is `...<YYYY-MM-DD>T<HH:MM:SS>.<ms>-<rand>.log.gz`; locate the
# first 'T' and use the 10 chars before it when they look like a date.
# Matches the GLOB in _migration_002 / get_log_accounting_counts so legacy
# and runtime parsing agree.
_FILE_DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})T")
_RUM_FILE_DATE_RE = re.compile(r"/(\d{4})/(\d{2})/(\d{2})/")


def _parse_file_date(file_name: str) -> str | None:
    """Return 'YYYY-MM-DD' parsed from filename or None if no match.

    Cheap regex on the basename — runs per-insert, called from the bulk
    INSERT in `insert_ingested_files`. Same semantics as the SQL backfill
    in `_migration_002_add_ingested_files_file_date`.
    """
    if not file_name:
        return None
    m = _FILE_DATE_RE.search(file_name)
    if m:
        return m.group(1)
    m_rum = _RUM_FILE_DATE_RE.search(file_name)
    if m_rum:
        return f"{m_rum.group(1)}-{m_rum.group(2)}-{m_rum.group(3)}"
    m_loose = re.search(r"(\d{4})-(\d{2})-(\d{2})", file_name)
    if m_loose:
        return f"{m_loose.group(1)}-{m_loose.group(2)}-{m_loose.group(3)}"
    return None


def _clear_ingested_filenames_cache(service_id: str | None = None) -> None:
    """Drop the dedup cache for one service or all services.

    Called from the pytest ``isolate_metadata_db`` fixture (every test gets a
    clean slate) and from ``teardown`` so deleted services don't keep
    phantom dedup state.
    """
    with _ingested_filenames_cache_lock:
        if service_id is None:
            _ingested_filenames_cache.clear()
        else:
            to_remove = [k for k in _ingested_filenames_cache if isinstance(k, tuple) and k[0] == service_id]
            for k in to_remove:
                _ingested_filenames_cache.pop(k, None)
            _ingested_filenames_cache.pop(service_id, None)


class InvalidServiceIdError(ValueError):
    """Raised by ``_validate_service_id_or_raise`` when ``service_id`` fails
    format validation. Every ``get_con`` / ``get_con_readonly`` call
    validates through this, Postgres included — a malformed ``service_id``
    still must not reach a WHERE/INSERT predicate unvalidated.

    Fastly service IDs are 22-character lowercase alphanumeric strings, but
    legacy fixtures and Admin-provisioned identifiers also use hyphens and
    mixed case, so we accept the union (``[A-Za-z0-9_-]{1,64}``). Anything
    outside that — non-ASCII characters, path separators, null bytes — is
    rejected at this chokepoint so every caller is safe (historically this
    also guarded the per-service SQLite file path from traversal; that
    file no longer exists, but the same input can still reach ``db_path()``,
    used by a handful of SQLite-flavored callers that predate this
    migration). The shared FastAPI exception handler in ``backend.main``
    converts this into a 422 instead of a 500.
    """


# Anchored, length-bounded. Hyphens and underscores allowed for legacy
# fixtures (e.g. "test-service-id"). 1-64 chars covers Fastly's 22-char
# native IDs with headroom for Admin-assigned suffixes.
_SERVICE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


def _validate_service_id_or_raise(service_id: str) -> None:
    if not isinstance(service_id, str):
        raise TypeError(f"service_id must be a string, got {type(service_id).__name__}: {service_id!r}")
    if not _SERVICE_ID_RE.fullmatch(service_id):
        raise InvalidServiceIdError(f"service_id must match {_SERVICE_ID_RE.pattern!r}; got {service_id!r}")


# ── Connection management ─────────────────────────────────────────────────────


def db_path(service_id: str) -> str:
    """Absolute path to where the per-service metadata SQLite file USED to
    live, before this module moved onto Postgres.

    Retained — not used by anything in this module's own connection path —
    because a handful of callers that predate the Postgres migration still
    build this path for their own (still-SQLite-shaped) purposes:
    ``alerts.list_alerts_cross_service`` (ATTACHes each service's file to a
    scratch in-memory SQLite DB) and ``repositories._base.attach_metadata_db``
    (ATTACHes it into a DuckDB query). Those call sites are unowned by this
    task; removing this function out from under them would break both at
    import time or first call. A non-string ``service_id`` would silently
    produce a junk path containing the object's repr (e.g.
    ``<...0x...>.metadata.db``); reject at the boundary so the bad caller is
    pinpointed immediately. A malformed-string ``service_id`` raises
    :class:`InvalidServiceIdError` for the same reason — see that class's
    docstring for the threat model.
    """
    _validate_service_id_or_raise(service_id)
    return os.path.join(_DATA_DIR, f"{service_id}.metadata.db")


def get_con(service_id: str) -> pg_connection.PgConnectionWrapper:
    """Return this thread's shared Postgres connection, tagged for ``service_id``.

    ``service_id`` is validated but otherwise unused for connection
    selection — every service shares ONE connection per thread (checked out
    once via :func:`pg_connection.get_pg_thread_connection` and held for the
    life of the thread), since rows are scoped by the ``service_id`` column
    migration 015 added, not by which physical database they live in. See
    :mod:`backend.core.metadata.pg_connection` for the full rationale.
    """
    _validate_service_id_or_raise(service_id)
    wrapper = pg_connection.get_pg_thread_connection()
    # Retag for the Live Query Monitor on every call — see
    # PgConnectionWrapper.__init__'s docstring on why this is safe for
    # a connection shared across services on one thread.
    wrapper._service_id = service_id
    return wrapper


def get_con_readonly(service_id: str) -> pg_connection.PgConnectionWrapper:
    """Return a short-lived read-only connection for the given service.

    This connection is not pooled and should be closed immediately (callers
    use ``contextlib.closing(...)``). ``.close()`` returns the connection to
    the pool rather than severing a socket.
    """
    _validate_service_id_or_raise(service_id)
    wrapper = pg_connection.get_pg_readonly_connection()
    wrapper._service_id = service_id
    return wrapper


def release_thread_connection() -> None:
    """Return the CALLING thread's ``get_con()`` connection, if any, so a
    thread that will never be reused doesn't permanently pin pool capacity.

    Returns the connection to the bounded ``METADATA_PG_POOL_MAX``-sized
    pool (see :func:`pg_connection.release_pg_thread_connection` for the
    full rationale — this is what a per-cron-tick heartbeat thread must call
    before exiting).
    """
    pg_connection.release_pg_thread_connection()


def close_all_connections() -> None:
    """Close every connection opened by ``get_con`` in any thread.

    Used by the pytest fixture in tests/conftest.py to drain connections
    opened on FastAPI TestClient worker threads — the fixture only has
    access to its own thread's state and would otherwise leak those.
    """
    pg_connection.close_all_pg_connections()


def teardown(service_id: str) -> None:
    """Clear this service's dedup cache. No-op otherwise.

    Called from ``backend/provision.py`` during service teardown. There is
    no per-service file to delete under Postgres (rows for every service
    share one database), and row-level deletion for a torn-down service is
    a data-retention decision the provisioning teardown flow does not
    currently make — this matches the no-op behavior Postgres mode already
    had before SQLite was removed. See this task's report for why a
    destructive row-deletion semantic was NOT added here without an
    explicit product decision to do so.
    """
    _clear_ingested_filenames_cache(service_id)


# ── Schema ────────────────────────────────────────────────────────────────────


_SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS ingest_ledger (
        service_id TEXT NOT NULL,
        object_key TEXT NOT NULL,
        etag TEXT,
        size_bytes INTEGER,
        status TEXT NOT NULL CHECK (status IN ('discovered','claimed','committed','quarantined','dead_letter')),
        claimed_by TEXT,
        claimed_at REAL,
        lease_generation INTEGER NOT NULL DEFAULT 0,
        committed_at REAL,
        published_at REAL,
        snapshot_ref TEXT,
        attempts INTEGER NOT NULL DEFAULT 0,
        last_error TEXT,
        error_kind TEXT,
        next_attempt_at REAL,
        discovered_at REAL,
        originating_task TEXT,
        originating_run_id INTEGER,
        outcome_counters TEXT NOT NULL DEFAULT '{}',
        raw_deleted_at REAL,
        raw_delete_claim_token TEXT,
        raw_delete_claimed_at REAL,
        PRIMARY KEY (service_id, object_key)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_ingest_ledger_claimed_scan ON ingest_ledger(service_id, status, claimed_at)",
    "CREATE INDEX IF NOT EXISTS idx_ingest_ledger_commit_scan "
    "ON ingest_ledger(service_id, status, committed_at, raw_deleted_at)",
    """
    CREATE TABLE IF NOT EXISTS job_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        service_id TEXT,
        job_name TEXT NOT NULL,
        started_at REAL NOT NULL,
        heartbeat_at REAL NOT NULL,
        lease_ttl_s INTEGER NOT NULL,
        finished_at REAL,
        status TEXT NOT NULL,
        detail TEXT
    )
    """,
    # Makes lease acquisition atomic under CONCURRENT writers (multiple
    # pods against one Postgres metadata backend): at most one 'running'
    # row can exist per (service_id, job_name) at a time. start_cron_run's
    # INSERT targets this index with ON CONFLICT ... DO NOTHING and reads
    # back rowcount to learn whether it won the lease — a single atomic
    # statement, unlike the old check-then-insert (SELECT count, then
    # INSERT), which raced under autocommit (see cron_log.py).
    """CREATE UNIQUE INDEX IF NOT EXISTS idx_job_runs_running_lease
        ON job_runs(service_id, job_name) WHERE status = 'running'""",
    """CREATE TABLE IF NOT EXISTS sources (
        name TEXT PRIMARY KEY,
        config TEXT,
        table_name TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    )""",
    """CREATE TABLE IF NOT EXISTS ingested_files (
        file_name TEXT,
        source_name TEXT,
        ingested_at TEXT DEFAULT (datetime('now')),
        row_count INTEGER,
        file_size_bytes INTEGER,
        error_count INTEGER DEFAULT 0,
        file_date DATE,
        table_name TEXT NOT NULL DEFAULT 'logs',
        PRIMARY KEY (file_name, source_name, table_name)
    )""",
    # Covers `/usage/prefill`'s source+range narrowing
    # (`WHERE source_name = ? AND ingested_at BETWEEN ? AND ?`) and the
    # bounded `list_unbackfilled_fastly_edge_files` scan (see :1128). The
    # previous `idx_ingested_files_source` indexed source_name alone — SQLite
    # had to walk every row for the matching source and filter ingested_at
    # in memory (~250ms per query on populated services). The composite
    # satisfies the range scan directly and is a strict superset for
    # source_name-only lookups (SQLite uses leading-column prefixes), so the
    # old index is redundant and dropped here. Index name matches the
    # by-name reference in `list_unbackfilled_fastly_edge_files`'s docstring.
    "CREATE INDEX IF NOT EXISTS idx_ingested_files_source_ingested_at ON ingested_files(source_name, ingested_at)",
    # Note: idx_ingested_files_source_date (companion index for per-day
    # usage queries) is created by _migration_002_add_ingested_files_file_date,
    # not here — _SCHEMA runs before migrations and a legacy DB upgrading
    # would fail on this CREATE INDEX (the file_date column doesn't exist
    # yet at that point). The migration is idempotent + runs for fresh DBs
    # too (apply_pending walks v1..LATEST on every init), so the index
    # always lands without _SCHEMA carrying it.
    "DROP INDEX IF EXISTS idx_ingested_files_source",
    # Earlier in this branch a redundant `idx_ingested_files_source_ts` was
    # added under a different name before discovering the existing
    # by-name reference above; clean it up so no service ends up with two
    # functionally identical composites.
    "DROP INDEX IF EXISTS idx_ingested_files_source_ts",
    # Single-row-per-service rollup maintained by ``insert_ingested_files``.
    # Without it, ``get_ingested_files_status_summary`` had to SUM(row_count)
    # + SUM(file_size_bytes) across the whole table on every cron tick —
    # ~4 s on services with >1 M rows since SQLite couldn't satisfy the SUMs
    # from any existing index. Lazy-bootstrapped from the full scan on first
    # read after upgrade; transactional delta updates after that.
    """CREATE TABLE IF NOT EXISTS ingested_files_summary (
        source_name TEXT PRIMARY KEY,
        file_count INTEGER NOT NULL DEFAULT 0,
        total_rows INTEGER NOT NULL DEFAULT 0,
        total_bytes INTEGER NOT NULL DEFAULT 0,
        count_with_bytes INTEGER NOT NULL DEFAULT 0,
        latest_file_name TEXT,
        last_ingested TEXT
    )""",
    # Atomic ingest manifest. A row is written BEFORE the buffer Parquet
    # appears on disk and deleted AFTER ingested_files is updated. On startup
    # the ingest loop sweeps this table: if the buffer file exists the row is
    # promoted (commit ingested_files, drop the in_flight row); if it is
    # missing the row is dropped without touching ingested_files (the buffer
    # write itself crashed — files will re-LIST on the next tick). Combined
    # with deterministic buffer filenames (sha256 of sorted source filenames)
    # this makes the ingest → buffer → metadata commit sequence crash-safe
    # without ever double-committing a row to Iceberg.
    """CREATE TABLE IF NOT EXISTS ingest_in_flight (
        buffer_filename TEXT NOT NULL,
        source_name TEXT NOT NULL,
        files_json TEXT NOT NULL,
        started_at TEXT DEFAULT (datetime('now')),
        table_name TEXT NOT NULL DEFAULT 'logs',
        PRIMARY KEY (buffer_filename, table_name)
    )""",
    "CREATE INDEX IF NOT EXISTS idx_in_flight_source ON ingest_in_flight(source_name)",
    """CREATE TABLE IF NOT EXISTS cron_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        service_id TEXT,
        task TEXT NOT NULL,
        started_at TEXT NOT NULL,
        duration_s REAL,
        status TEXT,
        error_message TEXT,
        files_downloaded INTEGER DEFAULT 0,
        files_deleted_fos INTEGER DEFAULT 0,
        rows_ingested INTEGER DEFAULT 0,
        corrupt_rows INTEGER DEFAULT 0,
        parquet_files_created INTEGER DEFAULT 0,
        parquet_files_optimized INTEGER DEFAULT 0,
        parquet_keys TEXT DEFAULT '[]',
        outcome_counters TEXT NOT NULL DEFAULT '{}',
        summary TEXT,
        log_output TEXT
    )""",
    "CREATE INDEX IF NOT EXISTS idx_cron_task_started ON cron_runs(task, started_at)",
    # Covers the every-5s running-check hot path in start_cron_run
    # (`WHERE task = ? AND status = 'running'`).
    "CREATE INDEX IF NOT EXISTS idx_cron_task_status ON cron_runs(task, status, started_at)",
    # Covers `/logs`'s unfiltered pagination
    # (`ORDER BY started_at DESC LIMIT ? OFFSET ?` with no `WHERE task`) and
    # `main.py`'s sync-status probe (`WHERE task='sync' AND status != 'running'
    # ORDER BY started_at DESC LIMIT 1`). Without it, SQLite falls back to a
    # TEMP B-TREE sort over the full table because `idx_cron_task_started`
    # requires a leading-`task` predicate to satisfy the ORDER BY.
    "CREATE INDEX IF NOT EXISTS idx_cron_started ON cron_runs(started_at DESC)",
    # Covers status polls and startup reap_running_cron_runs without a task filter
    "CREATE INDEX IF NOT EXISTS idx_cron_status ON cron_runs(status)",
    # Covers per-service task lookups and loose index scans
    "CREATE INDEX IF NOT EXISTS idx_cron_runs_service_task_started ON cron_runs(service_id, task, started_at DESC, id DESC)",
    # Covers per-service started_at pagination and probes
    "CREATE INDEX IF NOT EXISTS idx_cron_runs_service_started ON cron_runs(service_id, started_at DESC, id DESC)",
    """CREATE TABLE IF NOT EXISTS asn_names (
        asn INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        fetched_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
    )""",
    """CREATE TABLE IF NOT EXISTS audit_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT NOT NULL DEFAULT (datetime('now')),
        source_name TEXT,
        event_type TEXT NOT NULL,
        details TEXT,
        actor TEXT
    )""",
    "CREATE INDEX IF NOT EXISTS idx_audit_source ON audit_logs(source_name)",
    """CREATE TABLE IF NOT EXISTS views (
        id TEXT PRIMARY KEY,
        service_id TEXT NOT NULL,
        name TEXT NOT NULL,
        filters_json TEXT NOT NULL,
        time_range_type TEXT,
        start_time TEXT,
        end_time TEXT,
        page TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    )""",
    """CREATE TABLE IF NOT EXISTS alerts (
        id TEXT PRIMARY KEY,
        service_id TEXT NOT NULL,
        name TEXT NOT NULL,
        category TEXT DEFAULT 'reliability',
        metric TEXT NOT NULL,
        evaluation_type TEXT DEFAULT 'absolute',
        evaluation_scope TEXT DEFAULT 'all',
        operator TEXT NOT NULL,
        threshold REAL NOT NULL,
        window_min REAL NOT NULL,
        comparison_period_min REAL,
        status_codes TEXT,
        webhook_url TEXT,
        channels_json TEXT DEFAULT '[]',
        zscore_threshold REAL DEFAULT 3.0,
        baseline_period_days INTEGER DEFAULT 7,
        enabled INTEGER DEFAULT 1,
        last_triggered_at TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    )""",
    # Admin-flagged sessions for the edge session-scoring system. Each row
    # is one (service, sid) tuple labeled good/bad/neutral by the admin.
    # Feeds backend.scoring.evaluate.evaluate() for matrix ROC-AUC; the
    # neutral label is captured for UI completeness but excluded from the
    # AUC computation (intentionally uncertain).
    """CREATE TABLE IF NOT EXISTS scoring_labels (
        id TEXT PRIMARY KEY,
        service_id TEXT NOT NULL,
        sid TEXT NOT NULL,
        label TEXT NOT NULL CHECK (label IN ('good', 'bad', 'neutral')),
        notes TEXT DEFAULT '',
        flagged_by TEXT,
        sample_ip TEXT,
        sample_ua TEXT,
        sample_url TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now'))
    )""",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_scoring_labels_svc_sid ON scoring_labels(service_id, sid)",
    "CREATE INDEX IF NOT EXISTS idx_scoring_labels_svc_label ON scoring_labels(service_id, label)",
    # Operator audit log specifically for scoring-config mutations.
    # Separate from audit_logs (which gets state_sync'd) because scoring-
    # audit is per-host operator-attribution data that should NOT mirror
    # to read_only analyst replicas.
    """CREATE TABLE IF NOT EXISTS scoring_audit (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT NOT NULL DEFAULT (datetime('now')),
        service_id TEXT NOT NULL,
        action TEXT NOT NULL,
        actor TEXT NOT NULL,
        details TEXT
    )""",
    "CREATE INDEX IF NOT EXISTS idx_scoring_audit_svc_ts ON scoring_audit(service_id, timestamp DESC)",
    # Plain timestamp index for the list_scoring_audit ORDER BY timestamp DESC
    # path when the service_id predicate is already satisfied — keeps the sort
    # itself indexed instead of falling back to a TEMP B-TREE on large audit
    # tables.
    "CREATE INDEX IF NOT EXISTS idx_scoring_audit_ts ON scoring_audit(timestamp DESC)",
    # service_id — not source_name — is the tenant discriminator, matching
    # every other table under a shared Postgres metadata database (ADR-15).
    # source_name is retained as informational only: readers used to filter
    # on it while binding service_id, which worked solely because the two
    # coincide today (src["name"] is the service id). See migration 020.
    """CREATE TABLE IF NOT EXISTS quarantined_files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        service_id TEXT,
        file_name TEXT NOT NULL,
        source_name TEXT NOT NULL,
        fos_key TEXT NOT NULL,
        error_key TEXT NOT NULL,
        meta_key TEXT NOT NULL,
        valid_rows INTEGER NOT NULL DEFAULT 0,
        corrupt_rows INTEGER NOT NULL DEFAULT 0,
        file_size_bytes INTEGER,
        error_size_bytes INTEGER,
        corrupt_samples TEXT,
        quarantined_at TEXT NOT NULL DEFAULT (datetime('now')),
        UNIQUE(service_id, file_name)
    )""",
    "CREATE INDEX IF NOT EXISTS idx_quarantined_at ON quarantined_files(service_id, quarantined_at)",
    """CREATE TABLE IF NOT EXISTS quarantine_evidence (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        service_id TEXT NOT NULL,
        source_type TEXT NOT NULL,
        original_key TEXT NOT NULL,
        line_ordinal INTEGER,
        byte_offset INTEGER,
        byte_length INTEGER NOT NULL,
        error_category TEXT NOT NULL,
        error_text TEXT NOT NULL,
        sha256 TEXT NOT NULL,
        file_path TEXT NOT NULL,
        quarantined_at TEXT NOT NULL DEFAULT (datetime('now'))
    )""",
    "CREATE INDEX IF NOT EXISTS idx_quarantine_evidence_fifo ON quarantine_evidence(service_id, quarantined_at, id)",
]

# No ``_init_schema`` here anymore: nothing calls ``get_con`` against a raw
# SQLite connection needing bootstrap, so the old SQLite-pool ``schema_fn``
# has no caller left. Postgres schema bootstrap goes through
# ``backend.core.metadata.pg_schema.ensure_pg_schema()`` (wired at API/Celery
# startup — see that module's docstring), which imports ``_SCHEMA`` above and
# runs it through ``pg_schema._to_postgres()``. ``_SCHEMA`` itself MUST stay
# SQLite-flavored and MUST NOT be deleted — it is the schema-of-record for
# both backends, not a SQLite-only artifact.


# ── Postgres schema DDL & boot-time ensure (folded from pg_schema.py) ─────────

import sys as _sys  # noqa: E402
import time as _t  # noqa: E402
from collections.abc import Callable  # noqa: E402
from typing import Any  # noqa: E402

from backend.utils.bounded_cache import BoundedTTLCache, CacheRegistry  # noqa: E402

CLICKHOUSE_CONTROL_DDL = (
    """
CREATE TABLE IF NOT EXISTS clickhouse_datasets (
    service_id TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    catalog_identity TEXT NOT NULL,
    source_table TEXT NOT NULL,
    source_snapshot BIGINT NOT NULL CHECK (source_snapshot >= 0),
    coverage_start TIMESTAMPTZ NOT NULL,
    coverage_end TIMESTAMPTZ NOT NULL CHECK (coverage_end >= coverage_start),
    expires_at TIMESTAMPTZ NOT NULL,
    expected_rows BIGINT NOT NULL CHECK (expected_rows BETWEEN 0 AND 1000000),
    canonical_digest TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (service_id, dataset_id)
)
""",
    """
CREATE TABLE IF NOT EXISTS clickhouse_artifacts (
    service_id TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    artifact_uri TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    canonical_digest TEXT NOT NULL,
    byte_size BIGINT NOT NULL CHECK (byte_size BETWEEN 1 AND 16777216),
    ordinal_start BIGINT NOT NULL CHECK (ordinal_start >= 0),
    row_count INTEGER NOT NULL CHECK (row_count BETWEEN 1 AND 10000),
    PRIMARY KEY (service_id, dataset_id, batch_id),
    UNIQUE (service_id, dataset_id, ordinal_start),
    FOREIGN KEY (service_id, dataset_id) REFERENCES clickhouse_datasets(service_id, dataset_id)
)
""",
    """
CREATE TABLE IF NOT EXISTS clickhouse_generations (
    service_id TEXT NOT NULL,
    generation TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    target_identity TEXT NOT NULL,
    selection_revision BIGINT NOT NULL,
    status TEXT NOT NULL DEFAULT 'building' CHECK (status IN ('building', 'active', 'retired')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    activated_at TIMESTAMPTZ,
    PRIMARY KEY (service_id, generation),
    UNIQUE (service_id, generation, dataset_id),
    FOREIGN KEY (service_id, dataset_id) REFERENCES clickhouse_datasets(service_id, dataset_id)
)
""",
    """
CREATE TABLE IF NOT EXISTS clickhouse_publications (
    service_id TEXT NOT NULL,
    generation TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'claimed', 'published', 'failed')),
    lease_fence BIGINT NOT NULL DEFAULT 0,
    lease_until TIMESTAMPTZ,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    first_error TEXT,
    last_error TEXT,
    published_at TIMESTAMPTZ,
    verified_digest TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (service_id, generation, batch_id),
    FOREIGN KEY (service_id, generation, dataset_id)
        REFERENCES clickhouse_generations(service_id, generation, dataset_id),
    FOREIGN KEY (service_id, dataset_id, batch_id)
        REFERENCES clickhouse_artifacts(service_id, dataset_id, batch_id)
)
""",
    """
CREATE TABLE IF NOT EXISTS clickhouse_service_selection (
    service_id TEXT PRIMARY KEY,
    generation TEXT,
    revision BIGINT NOT NULL DEFAULT 0,
    FOREIGN KEY (service_id, generation) REFERENCES clickhouse_generations(service_id, generation)
)
""",
    """
CREATE TABLE IF NOT EXISTS clickhouse_schema_state (
    target_identity TEXT PRIMARY KEY,
    schema_version INTEGER NOT NULL,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
)
""",
)
CLICKHOUSE_TABLES_DDL = CLICKHOUSE_CONTROL_DDL

_SLOW_QUERIES_DDL = """
CREATE TABLE IF NOT EXISTS slow_queries (
    id SERIAL PRIMARY KEY,
    query_id INTEGER NOT NULL,
    db_type TEXT NOT NULL,
    service_id TEXT,
    started_at_utc DOUBLE PRECISION NOT NULL,
    ended_at_utc DOUBLE PRECISION NOT NULL,
    duration_ms DOUBLE PRECISION NOT NULL,
    outcome TEXT NOT NULL,
    sql_preview TEXT NOT NULL,
    sql_full TEXT,
    sql_len INTEGER NOT NULL,
    attr_kind TEXT NOT NULL,
    attr_label TEXT NOT NULL,
    attr_principal_id TEXT,
    attr_caller_qualname TEXT NOT NULL,
    attr_caller_file TEXT NOT NULL,
    attr_request_path TEXT,
    attr_request_id TEXT,
    attr_cron_job TEXT,
    attr_cron_run_id TEXT,
    attr_pool_slot TEXT,
    error_type TEXT,
    error_message TEXT,
    peak_memory_mb DOUBLE PRECISION
)
"""

_SLOW_QUERIES_INDEX_DDL = (
    "CREATE INDEX IF NOT EXISTS idx_slow_queries_service_started_dur ON slow_queries (service_id, started_at_utc, duration_ms)",
    "CREATE INDEX IF NOT EXISTS idx_slow_queries_service_duration ON slow_queries (service_id, duration_ms DESC, started_at_utc DESC)",
    "CREATE INDEX IF NOT EXISTS idx_slow_queries_started_at ON slow_queries (started_at_utc DESC)",
)


_METRIC_SNAPSHOTS_DDL = """
CREATE TABLE IF NOT EXISTS metric_snapshots (
    ts          TEXT NOT NULL,
    metric      TEXT NOT NULL,
    service_id  TEXT NOT NULL DEFAULT '',
    task        TEXT NOT NULL DEFAULT '',
    value       DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (metric, service_id, task, ts)
)
"""

_METRIC_SNAPSHOTS_INDEX_DDL = (
    "CREATE INDEX IF NOT EXISTS idx_metric_lookup ON metric_snapshots (metric, ts)",
    "CREATE INDEX IF NOT EXISTS idx_metric_snapshots_ts ON metric_snapshots (ts)",
)

_SEED_INITIAL_TOS_DDL = """
INSERT INTO share_tos_versions (version, text, published_at)
VALUES (
    'v1',
    'I acknowledge that I am viewing third-party operational log data, that my access is logged, and that I will not retain, redistribute, or use this data outside the scope of my engagement.',
    to_char(current_timestamp AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
)
ON CONFLICT (version) DO NOTHING
"""

_NGWAF_BOTS_DDL = """
CREATE TABLE IF NOT EXISTS ngwaf_bots (
    waf_req_id         TEXT PRIMARY KEY,
    bot_name           TEXT,
    category           TEXT,
    wellknown_bot_id   TEXT,
    wellknown_bot_name TEXT,
    synced_at          TEXT
)
"""

_NGWAF_SYNC_STATE_DDL = """
CREATE TABLE IF NOT EXISTS ngwaf_sync_state (
    workspace_id          TEXT PRIMARY KEY,
    last_timestamp_synced TEXT
)
"""

_RDNS_DDL = """
CREATE TABLE IF NOT EXISTS rdns (
    ip              TEXT PRIMARY KEY,
    hostname        TEXT,
    status          TEXT,
    fcrdns_verified INTEGER DEFAULT 0,
    looked_up_at    TEXT
)
"""

_RDNS_INDEX_DDL = """
CREATE INDEX IF NOT EXISTS idx_rdns_status_looked_up_at ON rdns (status, looked_up_at)
"""

_INGEST_LEDGER_ALTERS = (
    "ALTER TABLE ingest_ledger ADD COLUMN IF NOT EXISTS lease_generation BIGINT NOT NULL DEFAULT 0",
    "ALTER TABLE ingest_ledger ADD COLUMN IF NOT EXISTS published_at DOUBLE PRECISION",
    "ALTER TABLE ingest_ledger ADD COLUMN IF NOT EXISTS error_kind TEXT",
    "ALTER TABLE ingest_ledger ADD COLUMN IF NOT EXISTS next_attempt_at DOUBLE PRECISION",
    "ALTER TABLE ingest_ledger ADD COLUMN IF NOT EXISTS originating_task TEXT",
    "ALTER TABLE ingest_ledger ADD COLUMN IF NOT EXISTS originating_run_id BIGINT",
    "ALTER TABLE ingest_ledger ADD COLUMN IF NOT EXISTS outcome_counters TEXT NOT NULL DEFAULT '{}'",
    "ALTER TABLE ingest_ledger ADD COLUMN IF NOT EXISTS raw_delete_claim_token TEXT",
    "ALTER TABLE ingest_ledger ADD COLUMN IF NOT EXISTS raw_delete_claimed_at DOUBLE PRECISION",
    "CREATE INDEX IF NOT EXISTS idx_ingest_ledger_claimed_scan ON ingest_ledger(service_id, status, claimed_at)",
    "CREATE INDEX IF NOT EXISTS idx_ingest_ledger_commit_scan "
    "ON ingest_ledger(service_id, status, committed_at, raw_deleted_at)",
    "CREATE INDEX IF NOT EXISTS idx_ingest_ledger_raw_delete_scan "
    "ON ingest_ledger(service_id, status, published_at, committed_at, raw_deleted_at, raw_delete_claimed_at)",
    "CREATE INDEX IF NOT EXISTS idx_ingest_ledger_retry_scan ON ingest_ledger(service_id, status, next_attempt_at)",
)
_INGEST_LEDGER_INDEX_NAMES = (
    "idx_ingest_ledger_claimed_scan",
    "idx_ingest_ledger_commit_scan",
    "idx_ingest_ledger_raw_delete_scan",
    "idx_ingest_ledger_retry_scan",
)

_QUARANTINED_FILES_ALTERS = ("ALTER TABLE quarantined_files ADD COLUMN IF NOT EXISTS reason_counts TEXT DEFAULT '{}'",)
_CRON_RUNS_ALTERS = ("ALTER TABLE cron_runs ADD COLUMN IF NOT EXISTS outcome_counters TEXT NOT NULL DEFAULT '{}'",)

_USAGE_LOG_WIDEN_ALTERS = (
    "ALTER TABLE usage_log ALTER COLUMN bytes TYPE BIGINT",
    "ALTER TABLE usage_log ALTER COLUMN count TYPE BIGINT",
    "ALTER TABLE usage_log_hourly_summary ALTER COLUMN bytes TYPE BIGINT",
    "ALTER TABLE usage_log_hourly_summary ALTER COLUMN count TYPE BIGINT",
)

_RACE_SQLSTATES = frozenset({"42P07", "42710", "23505"})

_ensured = False
_ensure_lock = threading.Lock()


def _to_postgres(sql: str) -> str:
    """Rewrite a SQLite DDL statement into Postgres dialect."""
    pg_sql = sql.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
    pg_sql = pg_sql.replace("INTEGER PRIMARY KEY", "SERIAL PRIMARY KEY")
    pg_sql = pg_sql.replace("SERIAL PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
    pg_sql = re.sub(r"\bREAL\b", "DOUBLE PRECISION", pg_sql)
    pg_sql = pg_sql.replace(
        "DEFAULT (datetime('now'))",
        "DEFAULT (to_char(current_timestamp AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'))",
    )
    pg_sql = pg_sql.replace(
        "DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))",
        "DEFAULT (to_char(current_timestamp AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'))",
    )
    pg_sql = pg_sql.replace(", STRICT", "")
    pg_sql = pg_sql.replace(" STRICT", "")
    return pg_sql


def pg_schema_statements() -> list[str]:
    """Every DDL statement the Postgres metadata store needs, in order."""
    from backend.core.metadata.usage_log import _SCHEMA as _USAGE_SCHEMA
    from backend.core.share_db import _SCHEMA as _SHARE_DB_SCHEMA
    from backend.high_scale.postgres_control import SCHEMA_DDL as HIGH_SCALE_CONTROL_DDL

    statements = [_to_postgres(sql) for sql in _SCHEMA + _USAGE_SCHEMA + _SHARE_DB_SCHEMA]
    statements = [
        statement
        for statement in statements
        if not any(f"CREATE INDEX IF NOT EXISTS {name}" in statement for name in _INGEST_LEDGER_INDEX_NAMES)
    ]
    statements.append(_SLOW_QUERIES_DDL)
    statements.extend(_SLOW_QUERIES_INDEX_DDL)
    statements.append(_METRIC_SNAPSHOTS_DDL)
    statements.extend(_METRIC_SNAPSHOTS_INDEX_DDL)
    statements.extend(_INGEST_LEDGER_ALTERS)
    statements.extend(_QUARANTINED_FILES_ALTERS)
    statements.extend(_CRON_RUNS_ALTERS)
    statements.extend(_USAGE_LOG_WIDEN_ALTERS)
    statements.extend(CLICKHOUSE_CONTROL_DDL)
    statements.append(HIGH_SCALE_CONTROL_DDL)
    statements.append(_SEED_INITIAL_TOS_DDL)
    statements.append(_NGWAF_BOTS_DDL)
    statements.append(_NGWAF_SYNC_STATE_DDL)
    statements.append(_RDNS_DDL)
    statements.append(_RDNS_INDEX_DDL)
    return statements


def _is_concurrent_create(exc: Exception) -> bool:
    return getattr(exc, "sqlstate", None) in _RACE_SQLSTATES


def apply_pg_schema(cur: Any) -> tuple[int, int]:
    """Apply the schema through ``cur``. Returns ``(applied, raced)``."""
    applied = 0
    raced = 0
    for pg_sql in pg_schema_statements():
        try:
            cur.execute(pg_sql)
            applied += 1
        except Exception as e:
            if _is_concurrent_create(e):
                raced += 1
                continue
            raise RuntimeError(
                f"failed to apply schema statement:\n{pg_sql.strip()}\n-> {type(e).__name__}: {e}"
            ) from e
    return applied, raced


def ensure_pg_schema(*, force: bool = False) -> bool:
    """Idempotently create the Postgres metadata schema. Once per process."""
    global _ensured

    if not pg_connection.is_postgres():
        return False
    if _ensured and not force:
        return False

    with _ensure_lock:
        if _ensured and not force:
            return False
        try:
            pool = pg_connection.get_pg_pool()
            with pool.connection() as conn, conn.cursor() as cur:
                applied, raced = apply_pg_schema(cur)
        except Exception as e:
            logger.critical(
                "[metadata] Postgres schema bootstrap FAILED — metadata queries will fail until this is "
                "resolved. Fix the database and restart, or run `uv run python scripts/setup_pg_schema.py`. "
                "Cause: %s",
                e,
            )
            return False
        _ensured = True

    logger.info(
        "[metadata] Postgres metadata schema ensured (%d statements applied, %d already created by another pod).",
        applied,
        raced,
    )
    return True


def reset_ensure_flag_for_tests() -> None:
    """Drop the once-per-process latch so a test can exercise the boot path."""
    global _ensured
    _ensured = False


# ── Metadata storage stats + age-based cleanup (folded from reconciliation.py) ─

DEFAULT_METADATA_RETENTION = {
    "usage_log_days": 1,
    "ingested_files_days": 1,
    "cron_runs_days": 7,
    "rollups_days": 90,
    "slow_queries_days": 7,
}


class _UsageLogDbProxy:
    def __getattr__(self, name: str) -> Any:
        from backend.core.metadata import usage_log as _ul

        return getattr(_ul, name)


_usage_log_db: Any = _UsageLogDbProxy()


def recompute_ingested_files_summary(con: Any, service_id: str) -> None:
    from backend.core.metadata import ingest_log as _il

    _il.recompute_ingested_files_summary(con, service_id)


_STATS_TABLES = (
    ("usage_log", "usage_log"),
    ("ingested_files", "ingested_files"),
    ("cron_runs", "cron_runs"),
    ("alerts", "alerts"),
    ("views", "saved_views"),
    ("audit_logs", "audit_log"),
    ("ingest_in_flight", "in_flight_buffers"),
    ("slow_queries", "slow_queries"),
    ("quarantined_files", "quarantined_files"),
)

_CLEANUP_TABLES = (
    ("usage_log", "usage_log_days", "timestamp"),
    ("ingested_files", "ingested_files_days", "ingested_at"),
    ("cron_runs", "cron_runs_days", "started_at"),
    ("slow_queries", "slow_queries_days", "started_at_utc"),
    ("quarantined_files", "quarantine_days", "quarantined_at"),
)
_SLOW_QUERIES_TABLE = "slow_queries"
_USAGE_LOG_TABLE = "usage_log"


def _retention_timestamp_expr(ts_col: str) -> str:
    """Return the timestamp expression used by a Postgres retention predicate."""
    return f"CAST({ts_col} AS TIMESTAMPTZ)"


_STATS_SOURCE_NAME_TABLES = frozenset({"ingested_files", "audit_logs", "ingest_in_flight"})
_STATS_SERVICE_ID_TABLES = frozenset({"cron_runs", "alerts", "views", "slow_queries", "quarantined_files"})


def _stats_table_count_query(sql_table: str, service_id: str) -> tuple[str, tuple[Any, ...]]:
    """Return a tenant-scoped row-count query and params for ``sql_table``."""
    if sql_table == "ingested_files":
        return (
            f"SELECT count(*) FROM {sql_table} WHERE (source_name = ? OR source_name = 'fos' OR source_name = '')",
            (service_id,),
        )
    if sql_table in _STATS_SOURCE_NAME_TABLES:
        return (
            f"SELECT count(*) FROM {sql_table} WHERE (source_name = ? OR source_name = '')",
            (service_id,),
        )
    if sql_table in _STATS_SERVICE_ID_TABLES:
        return (
            f"SELECT count(*) FROM {sql_table} WHERE (service_id = ? OR service_id = '' OR service_id IS NULL)",
            (service_id,),
        )
    return f"SELECT count(*) FROM {sql_table}", ()


def _tenant_where_clause(table: str, service_id: str) -> tuple[str, tuple[Any, ...]]:
    """Return a SQL predicate and params scoping a cleanup table to ``service_id``."""
    if table == "ingested_files":
        return "(source_name = ? OR source_name IN ('fos', 'test_service') OR source_name IS NULL)", (service_id,)
    return "(service_id = ? OR service_id IS NULL)", (service_id,)


def _open_usage_log(service_id: str) -> Any | None:
    """Open the per-service usage_log connection if available; else None."""
    try:
        return _usage_log_db.open_readonly(service_id)
    except Exception:
        return None


_storage_stats_cache: BoundedTTLCache = BoundedTTLCache(maxsize=256, ttl_seconds=60.0)
CacheRegistry.register("metadata.reconciliation._storage_stats_cache", _storage_stats_cache)


def invalidate_storage_stats_cache(service_id: str) -> None:
    """Invalidate the storage stats cache for the given service."""
    _storage_stats_cache.pop(service_id, None)


def get_metadata_storage_stats(service_id: str, *, force: bool = False) -> dict:
    """Per-table row count + estimated bytes for this service's metadata store."""
    if not force:
        cached: dict | None = _storage_stats_cache.get(service_id)
        if cached is not None:
            return cached

    import contextlib

    out: dict[str, dict] = {}
    usage_log_con = _open_usage_log(service_id)
    if usage_log_con is None:
        out[_USAGE_LOG_TABLE] = {"rows": 0, "bytes": 0}
    else:
        try:
            rows = (
                usage_log_con.execute(
                    "SELECT sum(count) FROM usage_log_hourly_summary WHERE service_id = ?",
                    (service_id,),
                ).fetchone()[0]
                or 0
            )
            row = usage_log_con.execute("SELECT pg_total_relation_size('usage_log')").fetchone()
            bytes_: int | None = int(row[0]) if row and row[0] is not None else 0
        except Exception:
            rows, bytes_ = 0, None
        finally:
            usage_log_con.close()
        out[_USAGE_LOG_TABLE] = {"rows": int(rows or 0), "bytes": bytes_}

    non_usage_tables = [(sql_table, out_key) for sql_table, out_key in _STATS_TABLES if sql_table != _USAGE_LOG_TABLE]
    with contextlib.closing(get_con_readonly(service_id)) as con:
        batched_ok = False
        try:
            union_parts: list[str] = []
            union_params: list[Any] = []
            for sql_table, out_key in non_usage_tables:
                count_sql, count_params = _stats_table_count_query(sql_table, service_id)
                union_parts.append(
                    f"SELECT ? AS out_key, ({count_sql}) AS rows, pg_total_relation_size('{sql_table}') AS bytes"
                )
                union_params.append(out_key)
                union_params.extend(count_params)
            batch_rows = con.execute(" UNION ALL ".join(union_parts), tuple(union_params)).fetchall()
            if batch_rows and len(batch_rows) == len(non_usage_tables):
                for br in batch_rows:
                    out[str(br[0])] = {
                        "rows": int(br[1] or 0),
                        "bytes": int(br[2]) if br[2] is not None else 0,
                    }
                batched_ok = True
        except Exception:
            batched_ok = False

        if not batched_ok:
            for sql_table, out_key in non_usage_tables:
                try:
                    count_sql, count_params = _stats_table_count_query(sql_table, service_id)
                    rows = con.execute(count_sql, count_params).fetchone()[0]
                except Exception:
                    continue
                try:
                    row = con.execute(f"SELECT pg_total_relation_size('{sql_table}')").fetchone()
                    bytes_ = int(row[0]) if row and row[0] is not None else 0
                except Exception:
                    bytes_ = None
                out[out_key] = {"rows": int(rows or 0), "bytes": bytes_}

    result = {
        "tables": out,
        "db_bytes": None,
        "db_path": None,
    }
    _storage_stats_cache[service_id] = result
    return result


def is_ingested_files_dedup_active(service_id: str) -> bool:
    """Return True when the ``ingested_files`` table is the active dedup gate."""
    from backend import config as svcconfig

    cfg = svcconfig.load_config(service_id) or {}
    return svcconfig.resolve_raw_delete_after(cfg)


def cleanup_metadata(
    service_id: str,
    retention: dict | None = None,
    on_event: Callable[[dict], None] | None = None,
) -> dict:
    """Delete rows older than the per-table retention window. VACUUM if any were deleted."""

    def _emit(event: dict) -> None:
        if on_event is None:
            return
        try:
            on_event(event)
        except Exception:
            logger.debug("_emit: on_event failed (ignored)", exc_info=True)

    cfg = {**DEFAULT_METADATA_RETENTION, **(retention or {})}

    if not is_ingested_files_dedup_active(service_id):
        configured = int(cfg.get("ingested_files_days") or 0)
        if configured > 0:
            _emit(
                {
                    "type": "status",
                    "message": (
                        f"ingested_files retention ({configured}d) ignored — "
                        "cron_sync.delete_after=false makes this table the dedup gate. "
                        "Trimming would cause full_sync to re-ingest aged-out files."
                    ),
                }
            )
        cfg["ingested_files_days"] = 0

    con = get_con(service_id)
    t0 = _t.time()

    total_steps = len(_CLEANUP_TABLES) + 2

    def _snapshot_counts() -> dict[str, int]:
        counts: dict[str, int] = {}
        for table, _, _ in _CLEANUP_TABLES:
            try:
                t_where, t_params = _tenant_where_clause(table, service_id)
                counts[table] = int(
                    con.execute(f"SELECT count(*) FROM {table} WHERE {t_where}", t_params).fetchone()[0] or 0
                )
            except Exception:
                counts[table] = 0
        return counts

    _emit({"type": "status", "message": "Reading current row counts…"})
    before = _snapshot_counts()

    deleted: dict[str, int] = {}
    for idx, (table, key, ts_col) in enumerate(_CLEANUP_TABLES, start=1):
        days = cfg.get(key)
        try:
            days_int = int(days) if days is not None else 0
        except (TypeError, ValueError):
            days_int = 0
        if days_int <= 0:
            deleted[table] = 0
            _emit(
                {
                    "type": "progress",
                    "current": idx,
                    "total": total_steps,
                    "message": f"{table}: retention disabled (0 days) — skipped",
                }
            )
            continue
        _emit({"type": "status", "message": f"Trimming {table} (older than {days_int}d)…"})
        _BATCH = 5_000
        t_where, t_params = _tenant_where_clause(table, service_id)
        try:
            total_n = 0
            while True:
                if table == _SLOW_QUERIES_TABLE:
                    cutoff_epoch = _t.time() - days_int * 86400
                    cur = con.execute(
                        f"DELETE FROM {table} WHERE ctid IN "
                        f"(SELECT ctid FROM {table} WHERE {t_where} AND {ts_col} < ? LIMIT ?)",
                        (*t_params, cutoff_epoch, _BATCH),
                    )
                elif table == "ingested_files":
                    from backend import config as svcconfig

                    cfg_svc = svcconfig.load_config(service_id) or {}
                    raw_retention_days = int(cfg_svc.get("log_retention_days", 90))
                    rum_days_int = max(days_int, raw_retention_days + 1)
                    timestamp_expr = _retention_timestamp_expr(ts_col)
                    cur = con.execute(
                        f"DELETE FROM {table} WHERE ctid IN "
                        f"(SELECT ctid FROM {table} WHERE {t_where} AND ("
                        f"(table_name NOT IN ('client_vitals', 'client_errors') AND {timestamp_expr} < CURRENT_TIMESTAMP - (? * INTERVAL '1 day')) "
                        f"OR (table_name IN ('client_vitals', 'client_errors') AND {timestamp_expr} < CURRENT_TIMESTAMP - (? * INTERVAL '1 day'))"
                        f") LIMIT ?)",
                        (*t_params, days_int, rum_days_int, _BATCH),
                    )
                else:
                    timestamp_expr = _retention_timestamp_expr(ts_col)
                    cur = con.execute(
                        f"DELETE FROM {table} WHERE ctid IN "
                        f"(SELECT ctid FROM {table} "
                        f"WHERE {t_where} AND {timestamp_expr} < CURRENT_TIMESTAMP - (? * INTERVAL '1 day') LIMIT ?)",
                        (*t_params, days_int, _BATCH),
                    )
                n = int(cur.rowcount or 0)
                total_n += n
                con.commit()
                if n < _BATCH:
                    break
            deleted[table] = total_n
            _emit(
                {
                    "type": "progress",
                    "current": idx,
                    "total": total_steps,
                    "message": f"{table}: deleted {deleted[table]:,} rows (kept rows ≤{days_int}d old)",
                }
            )
        except Exception as e:
            logger.warning("[metadata_cleanup] %s: skip %s — %s", service_id, table, e)
            deleted[table] = 0
            _emit(
                {
                    "type": "progress",
                    "current": idx,
                    "total": total_steps,
                    "message": f"{table}: skipped ({e})",
                }
            )

    if deleted.get("ingested_files", 0) > 0:
        try:
            recompute_ingested_files_summary(con, service_id)
        except Exception as e:
            logger.warning(
                "[metadata_cleanup] %s: ingested_files_summary recompute failed — %s",
                service_id,
                e,
            )

    vacuumed = False
    _emit(
        {
            "type": "progress",
            "current": len(_CLEANUP_TABLES) + 1,
            "total": total_steps,
            "message": "Cleanup complete (Postgres autovacuum manages storage)",
        }
    )

    after = _snapshot_counts()
    _emit(
        {
            "type": "progress",
            "current": total_steps,
            "total": total_steps,
            "message": f"Final counts: {', '.join(f'{t}={n:,}' for t, n in after.items())}",
        }
    )

    rollups_deleted = 0
    try:
        rollups_days = int(cfg.get("rollups_days") or 0)
    except (TypeError, ValueError):
        rollups_days = 0
    if rollups_days > 0:
        try:
            from backend.core import rollups as _rollups
            from backend.core.duckdb import get_source_for_service

            src = get_source_for_service(service_id)
            if src is not None:
                rollups_deleted = _rollups.cleanup_old_rollups(service_id, src, rollups_days)
                if rollups_deleted:
                    _emit(
                        {
                            "type": "status",
                            "message": f"Rollups: dropped {rollups_deleted} hour-dir(s) older than {rollups_days}d",
                        }
                    )
        except Exception as e:
            logger.warning("[metadata_cleanup] %s: rollups cleanup skipped — %s", service_id, e)

    invalidate_storage_stats_cache(service_id)
    return {
        "deleted": deleted,
        "before": before,
        "after": after,
        "vacuumed": vacuumed,
        "rollups_deleted": rollups_deleted,
        "duration_s": round(_t.time() - t0, 3),
    }


_sys.modules["backend.core.metadata.pg_schema"] = _sys.modules[__name__]
_sys.modules["backend.core.metadata.reconciliation"] = _sys.modules[__name__]
