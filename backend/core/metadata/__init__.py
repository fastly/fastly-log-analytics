"""Per-service operational metadata store, backed by SQLite.

DuckDB is reserved for analytical queries over Iceberg log data. Everything
else — alerts, saved views, audit logs, ingested-file dedup tracking, cron run
history, ASN name cache, source registration, FOS/CDN usage telemetry — lives
here, in a per-service SQLite file at ``data/services/{service_id}.metadata.db``.

Why per-service: SQLite's writer lock is per-file even in WAL mode. With many
services ingesting concurrently, a single global file would serialise every
ingest's `ingested_files` write. Per-file isolation also makes service
teardown a single ``rm`` and bounds blast radius on corruption.

Concurrency model: thread-local connections (sqlite3 connections are not
thread-safe) keyed by ``(thread, service_id)``. WAL + ``synchronous=NORMAL``
gives readers freedom from writer locks within a single file.

This package is the carved successor to the historical
``backend.core.metadata_db`` monolith. The functions are split across
concern-specific submodules (``base``, ``alerts``, ``views``, ``state``,
``ingest_log``, ``cron_log``, ``asn_cache``, ``usage_log``,
``reconciliation``) and re-exported here so existing call sites that import
this package — or the ``backend.core.metadata_db`` shim that mirrors this
surface — continue to work unchanged.
"""

from __future__ import annotations

# Alerts CRUD.
from backend.core.metadata.alerts import (
    count_alerts,
    delete_alert,
    list_alerts,
    save_alert,
    toggle_alert,
    update_alert_last_triggered,
)

# ASN-name cache.
from backend.core.metadata.asn_cache import (
    asn_ints_for_search,
    lookup_asn_names,
    upsert_asn_names,
)

# Base: connection management, schema, dedup cache, parse helpers.
from backend.core.metadata.base import (
    _DATA_DIR,
    _FILE_DATE_RE,
    _ORPHAN_THRESHOLD_MINS,
    _SCHEMA,
    _all_connections,
    _all_connections_lock,
    _clear_ingested_filenames_cache,
    _connections,
    _ingested_filenames_cache,
    _ingested_filenames_cache_lock,
    _init_lock,
    _init_schema,
    _initialized,
    _local,
    _parse_file_date,
    close_all_connections,
    db_path,
    get_con,
    teardown,
)

# Cron run history + scoring audit.
from backend.core.metadata.cron_log import (
    cron_busy,
    cron_summary_for_tasks,
    delete_cron_run,
    get_cron_run_status,
    get_cron_runs,
    latest_cron_per_task,
    list_scoring_audit,
    log_cron_run,
    prune_scoring_audit,
    purge_cron_runs,
    reap_running_cron_runs,
    record_scoring_audit,
    start_cron_run,
    update_cron_duration,
)

# Ingested-files tracking + activity reporting.
from backend.core.metadata.ingest_log import (
    _bootstrap_ingested_files_summary,
    clear_in_flight,
    get_ingested_filenames,
    get_ingested_files_status_summary,
    get_latest_reconciliation_ts,
    get_locally_compacted_basenames,
    get_log_accounting_counts,
    get_log_activity,
    get_node_count_avg,
    get_storage_stats_window,
    insert_ingested_files,
    list_in_flight,
    list_ingested_files,
    list_ingested_files_for_status,
    list_unbackfilled_fastly_edge_files,
    record_in_flight,
    register_locally_compacted,
)

# Metadata cleanup + storage stats.
from backend.core.metadata.reconciliation import (
    _CLEANUP_TABLES,
    _STATS_TABLES,
    cleanup_metadata,
    get_metadata_storage_stats,
    is_ingested_files_dedup_active,
)

# Audit log + applied data migration tracking.
from backend.core.metadata.state import (
    export_audit,
    get_audit_logs,
    list_applied_data_migrations,
    list_audit,
    merge_audit_for_service,
    record_applied_data_migration,
    record_audit,
    replace_audit_for_service,
)

# Source registry + usage telemetry.
from backend.core.metadata.usage_log import (
    DEFAULT_METADATA_RETENTION,
    USAGE_LOG_HOURLY_BACKFILL_NAME,
    _ensure_usage_log_hourly_backfilled,
    _query_usage_log_aggregate_rollup,
    _usage_log_backfill_lock,
    _usage_log_backfilled,
    clear_usage_log,
    get_source_by_name,
    get_usage_logs,
    log_synthetic_usage,
    log_usage_calls,
    purge_usage_log,
    reconcile_fastly_stats,
    register_source,
)

# Saved-dashboard-view CRUD.
from backend.core.metadata.views import (
    delete_view,
    list_views,
    replace_views_for_service,
    save_view,
    upsert_views_for_service,
)

__all__ = [
    # Connection / schema (public)
    "db_path",
    "get_con",
    "close_all_connections",
    "teardown",
    # Alerts
    "list_alerts",
    "count_alerts",
    "save_alert",
    "toggle_alert",
    "delete_alert",
    "update_alert_last_triggered",
    # Views
    "list_views",
    "save_view",
    "delete_view",
    "replace_views_for_service",
    "upsert_views_for_service",
    # Audit + data migration tracking
    "record_audit",
    "list_audit",
    "get_audit_logs",
    "export_audit",
    "replace_audit_for_service",
    "merge_audit_for_service",
    "list_applied_data_migrations",
    "record_applied_data_migration",
    # Ingested files
    "get_ingested_filenames",
    "list_ingested_files",
    "list_ingested_files_for_status",
    "get_ingested_files_status_summary",
    "get_log_accounting_counts",
    "get_storage_stats_window",
    "list_unbackfilled_fastly_edge_files",
    "get_latest_reconciliation_ts",
    "register_locally_compacted",
    "get_locally_compacted_basenames",
    "insert_ingested_files",
    "record_in_flight",
    "clear_in_flight",
    "list_in_flight",
    "get_log_activity",
    "get_node_count_avg",
    # Cron runs
    "start_cron_run",
    "log_cron_run",
    "update_cron_duration",
    "delete_cron_run",
    "purge_cron_runs",
    "record_scoring_audit",
    "list_scoring_audit",
    "prune_scoring_audit",
    "get_cron_run_status",
    "get_cron_runs",
    "latest_cron_per_task",
    "reap_running_cron_runs",
    "cron_busy",
    "cron_summary_for_tasks",
    # ASN cache
    "lookup_asn_names",
    "upsert_asn_names",
    "asn_ints_for_search",
    # Sources
    "register_source",
    "get_source_by_name",
    # Usage log
    "log_usage_calls",
    "log_synthetic_usage",
    "reconcile_fastly_stats",
    "purge_usage_log",
    "clear_usage_log",
    "USAGE_LOG_HOURLY_BACKFILL_NAME",
    "get_usage_logs",
    "DEFAULT_METADATA_RETENTION",
    # Reconciliation / cleanup
    "get_metadata_storage_stats",
    "is_ingested_files_dedup_active",
    "cleanup_metadata",
    # Module-level state hooks used by tests + state_sync
    "_clear_ingested_filenames_cache",
    "_DATA_DIR",
    "_initialized",
    "_local",
    "_init_lock",
    "_init_schema",
    "_SCHEMA",
    "_all_connections",
    "_all_connections_lock",
    "_connections",
    "_ingested_filenames_cache",
    "_ingested_filenames_cache_lock",
    "_parse_file_date",
    "_FILE_DATE_RE",
    "_ORPHAN_THRESHOLD_MINS",
    "_bootstrap_ingested_files_summary",
    "_ensure_usage_log_hourly_backfilled",
    "_query_usage_log_aggregate_rollup",
    "_usage_log_backfilled",
    "_usage_log_backfill_lock",
    "_STATS_TABLES",
    "_CLEANUP_TABLES",
]
