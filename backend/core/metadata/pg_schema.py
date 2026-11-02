"""PostgreSQL metadata schema DDL, boot-time ensure, and age-based retention cleanup."""

from __future__ import annotations

import contextlib
import logging
import re
import sys
import threading
import time as _t
from collections.abc import Callable
from typing import Any

from backend.core.metadata import pg_pool as pg_connection
from backend.core.metadata.pg_pool import (
    get_con,
    get_con_readonly,
)
from backend.utils.bounded_cache import BoundedTTLCache, CacheRegistry

logger = logging.getLogger(__name__)


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
    # bounded `list_unbackfilled_fastly_edge_files` scan. The composite also
    # serves source_name-only lookups through its leading column.
    "CREATE INDEX IF NOT EXISTS idx_ingested_files_source_ingested_at ON ingested_files(source_name, ingested_at)",
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
    # without ever double-committing a row to DuckLake.
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
    # `main.py`'s sync-status probe (`WHERE task='log_discovery' AND status != 'running'
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
    # coincide today (src["name"] is the service id).
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
        reason_counts TEXT DEFAULT '{}',
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

_INGEST_LEDGER_INDEXES = (
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
    statements.extend(_INGEST_LEDGER_INDEXES)
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
        setattr(sys.modules[__name__], "_ensured", True)

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
    setattr(sys.modules[__name__], "_ensured", False)


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
