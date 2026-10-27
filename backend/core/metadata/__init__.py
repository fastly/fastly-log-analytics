"""Per-service operational metadata store, backed by Postgres.

DuckDB is reserved for analytical queries over DuckLake log data. Everything
else — alerts, saved views, audit logs, ingested-file dedup tracking, cron run
history, ASN name cache, source registration, FOS/CDN usage telemetry — lives
here, in a single shared Postgres database (``METADATA_DSN``), with every
service-scoped table carrying a ``service_id`` column.

Concurrency model: one pooled connection per thread (``pg_pool``'s
autocommit=True pool), shared across every service that thread touches — see
:mod:`backend.core.metadata.pg_pool` for the full rationale.

This package is decomposed into concern-specific submodules (``pg_pool``,
``pg_schema``, ``admin_state``, ``alerts_db``, ``state_sync``, ``fos_usage``,
``slow_queries_db``, ``cron_runs``, ``ingest_log``, ``ingest_ledger``) with
thin re-export shims at ``base`` and ``usage_log``. Writes to shared state
attributes are mirrored across ``metadata``, ``base``, ``usage_log``, and the
underlying submodules via the ``_ShimModule`` proxy installed below.
"""

from __future__ import annotations

import sys
from types import ModuleType

from backend.core.metadata import (
    admin_state as _admin_state,
)
from backend.core.metadata import (
    alerts_db as _alerts_db,
)
from backend.core.metadata import (
    base as _base,
)
from backend.core.metadata import (
    cron_runs as _cron_runs,
)
from backend.core.metadata import (
    fos_usage as _fos_usage,
)
from backend.core.metadata import (
    ingest_log as _ingest_log,
)
from backend.core.metadata import (
    pg_pool as _pg_pool,
)
from backend.core.metadata import (
    pg_schema as _pg_schema,
)
from backend.core.metadata import (
    slow_queries_db as _slow_queries_db,
)
from backend.core.metadata import (
    state_sync as _state_sync,
)
from backend.core.metadata import (
    usage_log as _usage_log,
)

state = _base
alerts = _base
asn_cache = _base
views = _base
cron_log = _usage_log
usage_log_db = _usage_log
quarantine = _ingest_log
pg_schema = _base
reconciliation = _base
pg_connection = _base

# Base: connection management, schema, dedup cache, parse helpers, state/alerts/views/audit.
from backend.core.metadata.base import (
    _CLEANUP_TABLES,
    _DATA_DIR,
    _FILE_DATE_RE,
    _SCHEMA,
    _STATS_TABLES,
    _clear_ingested_filenames_cache,
    _ingested_filenames_cache,
    _ingested_filenames_cache_lock,
    _parse_file_date,
    asn_ints_for_search,
    cleanup_metadata,
    close_all_connections,
    count_alerts,
    db_path,
    delete_alert,
    delete_view,
    export_audit,
    get_audit_logs,
    get_con,
    get_con_readonly,
    get_metadata_storage_stats,
    is_ingested_files_dedup_active,
    list_alerts,
    list_alerts_cross_service,
    list_audit,
    list_views,
    lookup_asn_names,
    record_audit,
    save_alert,
    save_view,
    teardown,
    toggle_alert,
    update_alert_last_triggered,
    upsert_asn_names,
)

# Ingested-files tracking + activity reporting + quarantined-file tracking.
from backend.core.metadata.ingest_log import (
    _bootstrap_ingested_files_summary,
    clear_in_flight,
    get_ingested_filenames,
    get_ingested_files_status_summary,
    get_latest_ingest_ts,
    get_latest_reconciliation_ts,
    get_log_accounting_counts,
    get_log_activity,
    get_node_count_avg,
    get_quarantine_storage_total,
    get_reclaimable_strand_filenames,
    get_rum_beacons_avg_per_day,
    get_rum_beacons_by_date,
    get_storage_stats_window,
    insert_ingested_files,
    list_in_flight,
    list_ingested_files,
    list_unbackfilled_fastly_edge_files,
    recompute_ingested_files_summary,
    record_in_flight,
)

# Source registry + usage telemetry + persistent slow-SQL history + cron run history.
from backend.core.metadata.usage_log import (
    DEFAULT_METADATA_RETENTION,
    _query_usage_log_aggregate_rollup,
    adaptive_stale_minutes,
    clear_slow_queries,
    clear_usage_log,
    count_slow_queries,
    cron_busy,
    delete_cron_run,
    finalize_cron_run_if_running,
    flush_slow_query_buffer,
    get_cron_run_result,
    get_cron_run_status,
    get_cron_runs,
    get_usage_logs,
    insert_slow_query,
    iter_usage_logs_chunks,
    latest_cron_per_task,
    list_scoring_audit,
    list_slow_queries,
    log_cron_run,
    log_synthetic_usage,
    log_usage_calls,
    purge_cron_runs,
    purge_usage_log,
    reap_running_cron_runs,
    reconcile_fastly_stats,
    record_scoring_audit,
    register_source,
    start_cron_run,
    update_cron_duration,
)

_ALL_SYNC_MODULES = (
    sys.modules[__name__],
    _base,
    _usage_log,
    _pg_pool,
    _pg_schema,
    _admin_state,
    _alerts_db,
    _state_sync,
    _fos_usage,
    _slow_queries_db,
    _cron_runs,
    _ingest_log,
)

_PROPAGATING = False


class _ShimModule(ModuleType):
    """Mirror writes for shared state across ``metadata``, ``base``, ``usage_log``,
    and the decomposed submodules so ``monkeypatch.setattr`` on any of them
    updates the live bindings in the implementation modules.
    """

    def __setattr__(self, name: str, value: object) -> None:
        global _PROPAGATING
        super().__setattr__(name, value)
        if _PROPAGATING or name.startswith("__"):
            return
        _PROPAGATING = True
        try:
            for mod in _ALL_SYNC_MODULES:
                if mod is not self and (
                    name in mod.__dict__
                    or (mod is _base and self in (_pg_pool, _pg_schema, _admin_state, _alerts_db, _state_sync))
                    or (mod is _usage_log and self in (_fos_usage, _slow_queries_db, _cron_runs))
                ):
                    # Special care: do not overwrite fos_usage._SCHEMA when pg_schema._SCHEMA changes
                    if name == "_SCHEMA" and mod is _fos_usage and self is not _fos_usage:
                        continue
                    ModuleType.__setattr__(mod, name, value)
        finally:
            _PROPAGATING = False

    def __delattr__(self, name: str) -> None:
        global _PROPAGATING
        super().__delattr__(name)
        if _PROPAGATING or name.startswith("__"):
            return
        _PROPAGATING = True
        try:
            for mod in _ALL_SYNC_MODULES:
                if mod is not self and name in mod.__dict__:
                    try:
                        ModuleType.__delattr__(mod, name)
                    except AttributeError:
                        pass
        finally:
            _PROPAGATING = False


for _mod in _ALL_SYNC_MODULES:
    _mod.__class__ = _ShimModule


__all__ = [
    "DEFAULT_METADATA_RETENTION",
    "_CLEANUP_TABLES",
    "_DATA_DIR",
    "_FILE_DATE_RE",
    "_SCHEMA",
    "_STATS_TABLES",
    "_bootstrap_ingested_files_summary",
    "_clear_ingested_filenames_cache",
    "_ingested_filenames_cache",
    "_ingested_filenames_cache_lock",
    "_parse_file_date",
    "_query_usage_log_aggregate_rollup",
    "adaptive_stale_minutes",
    "asn_ints_for_search",
    "cleanup_metadata",
    "clear_in_flight",
    "clear_slow_queries",
    "clear_usage_log",
    "close_all_connections",
    "count_alerts",
    "count_slow_queries",
    "cron_busy",
    "db_path",
    "delete_alert",
    "delete_cron_run",
    "delete_view",
    "export_audit",
    "finalize_cron_run_if_running",
    "flush_slow_query_buffer",
    "get_audit_logs",
    "get_con",
    "get_con_readonly",
    "get_cron_run_result",
    "get_cron_run_status",
    "get_cron_runs",
    "get_ingested_filenames",
    "get_ingested_files_status_summary",
    "get_latest_ingest_ts",
    "get_latest_reconciliation_ts",
    "get_log_accounting_counts",
    "get_log_activity",
    "get_metadata_storage_stats",
    "get_node_count_avg",
    "get_quarantine_storage_total",
    "get_reclaimable_strand_filenames",
    "get_rum_beacons_avg_per_day",
    "get_rum_beacons_by_date",
    "get_storage_stats_window",
    "get_usage_logs",
    "insert_ingested_files",
    "insert_slow_query",
    "is_ingested_files_dedup_active",
    "iter_usage_logs_chunks",
    "latest_cron_per_task",
    "list_alerts",
    "list_alerts_cross_service",
    "list_audit",
    "list_in_flight",
    "list_ingested_files",
    "list_scoring_audit",
    "list_slow_queries",
    "list_unbackfilled_fastly_edge_files",
    "list_views",
    "log_cron_run",
    "log_synthetic_usage",
    "log_usage_calls",
    "lookup_asn_names",
    "purge_cron_runs",
    "purge_usage_log",
    "reap_running_cron_runs",
    "recompute_ingested_files_summary",
    "reconcile_fastly_stats",
    "record_audit",
    "record_in_flight",
    "record_scoring_audit",
    "register_source",
    "save_alert",
    "save_view",
    "start_cron_run",
    "teardown",
    "toggle_alert",
    "update_alert_last_triggered",
    "update_cron_duration",
    "upsert_asn_names",
]
