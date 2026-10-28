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
``slow_queries_db``, ``cron_runs``, ``ingest_log``, ``quarantine_evidence``,
``ingest_ledger``) with thin re-export shims at ``base`` and ``usage_log``.
Writes to shared state attributes are mirrored across ``metadata``, ``base``,
``usage_log``, and the underlying submodules via ``install_shim_module``.
"""

from __future__ import annotations

import sys

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
    quarantine_evidence as _quarantine_evidence,
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
from backend.utils.module_shim import install_shim_module

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
    QUARANTINE_ITEM_CAP,
    _bootstrap_ingested_files_summary,
    capture_evidence,
    clear_in_flight,
    count_quarantine_evidence,
    delete_quarantine_evidence,
    get_ingested_filenames,
    get_ingested_files_status_summary,
    get_latest_ingest_ts,
    get_latest_reconciliation_ts,
    get_log_accounting_counts,
    get_log_activity,
    get_node_count_avg,
    get_quarantine_evidence,
    get_quarantine_evidence_summary,
    get_quarantine_storage_total,
    get_quarantine_summary,
    get_reclaimable_strand_filenames,
    get_rum_beacons_avg_per_day,
    get_rum_beacons_by_date,
    get_storage_stats_window,
    insert_ingested_files,
    insert_quarantine_evidence,
    list_in_flight,
    list_ingested_files,
    list_quarantine_evidence,
    list_unbackfilled_fastly_edge_files,
    purge_evidence,
    read_evidence,
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
    _quarantine_evidence,
)

_ShimModule = install_shim_module(
    __name__,
    _ALL_SYNC_MODULES[1:],
    only_existing_attrs=True,
    sync_filter=lambda src, dst, name, _val: not (name == "_SCHEMA" and dst is _fos_usage and src is not _fos_usage),
)


__all__ = [
    "DEFAULT_METADATA_RETENTION",
    "QUARANTINE_ITEM_CAP",
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
    "capture_evidence",
    "cleanup_metadata",
    "clear_in_flight",
    "clear_slow_queries",
    "clear_usage_log",
    "close_all_connections",
    "count_alerts",
    "count_quarantine_evidence",
    "count_slow_queries",
    "cron_busy",
    "db_path",
    "delete_alert",
    "delete_cron_run",
    "delete_quarantine_evidence",
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
    "get_quarantine_evidence",
    "get_quarantine_evidence_summary",
    "get_quarantine_storage_total",
    "get_quarantine_summary",
    "get_reclaimable_strand_filenames",
    "get_rum_beacons_avg_per_day",
    "get_rum_beacons_by_date",
    "get_storage_stats_window",
    "get_usage_logs",
    "insert_ingested_files",
    "insert_quarantine_evidence",
    "insert_slow_query",
    "is_ingested_files_dedup_active",
    "iter_usage_logs_chunks",
    "latest_cron_per_task",
    "list_alerts",
    "list_alerts_cross_service",
    "list_audit",
    "list_in_flight",
    "list_ingested_files",
    "list_quarantine_evidence",
    "list_scoring_audit",
    "list_slow_queries",
    "list_unbackfilled_fastly_edge_files",
    "list_views",
    "log_cron_run",
    "log_synthetic_usage",
    "log_usage_calls",
    "lookup_asn_names",
    "purge_cron_runs",
    "purge_evidence",
    "purge_usage_log",
    "read_evidence",
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
