"""Per-service operational metadata store, backed by Postgres.

DuckDB is reserved for analytical queries over Iceberg log data. Everything
else — alerts, saved views, audit logs, ingested-file dedup tracking, cron run
history, ASN name cache, source registration, FOS/CDN usage telemetry — lives
here, in a single shared Postgres database (``METADATA_DSN``), with every
service-scoped table carrying a ``service_id`` column.

Concurrency model: one pooled connection per thread (``pg_connection``'s
autocommit=True pool), shared across every service that thread touches — see
:mod:`backend.core.metadata.pg_connection` for the full rationale (superseding
the historical per-service-SQLite-file model this package used before the
SQLite -> Postgres metadata migration).

This package is the carved successor to the historical
``backend.core.metadata_db`` monolith. The functions are split across
concern-specific submodules (``base``, ``alerts``, ``views``, ``state``,
``ingest_log``, ``cron_log``, ``asn_cache``, ``usage_log``,
``reconciliation``) and re-exported here. Writes to shared state attributes
(``_DATA_DIR``, ``_SCHEMA``, etc.) are mirrored onto ``metadata.base`` via the
``_ShimModule`` proxy installed below — required because tests and
``state_sync`` use ``monkeypatch.setattr(metadata, "X", ...)`` and expect the
live ``get_con`` bindings to swap.
"""

from __future__ import annotations

import sys
from types import ModuleType

from backend.core.metadata import base as _base
from backend.core.metadata import ingest_log as _ingest_log
from backend.core.metadata import state as _state
from backend.core.metadata import usage_log as _usage_log

alerts = _state
asn_cache = _state
views = _state
usage_log_db = _usage_log
quarantine = _ingest_log

# Base: connection management, schema, dedup cache, parse helpers.
from backend.core.metadata.base import (
    _DATA_DIR,
    _FILE_DATE_RE,
    _SCHEMA,
    _clear_ingested_filenames_cache,
    _ingested_filenames_cache,
    _ingested_filenames_cache_lock,
    _parse_file_date,
    close_all_connections,
    db_path,
    get_con,
    get_con_readonly,
    teardown,
)

# Cron run history + scoring audit.
from backend.core.metadata.cron_log import (
    adaptive_stale_minutes,
    cron_busy,
    delete_cron_run,
    finalize_cron_run_if_running,
    get_cron_run_result,
    get_cron_run_status,
    get_cron_runs,
    latest_cron_per_task,
    list_scoring_audit,
    log_cron_run,
    purge_cron_runs,
    reap_running_cron_runs,
    record_scoring_audit,
    start_cron_run,
    update_cron_duration,
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

# Metadata cleanup + storage stats.
from backend.core.metadata.reconciliation import (
    _CLEANUP_TABLES,
    _STATS_TABLES,
    cleanup_metadata,
    get_metadata_storage_stats,
    is_ingested_files_dedup_active,
)

# Audit log, ASN-name cache, saved-dashboard-view, and alert-rule CRUD.
from backend.core.metadata.state import (
    asn_ints_for_search,
    count_alerts,
    delete_alert,
    delete_view,
    export_audit,
    get_audit_logs,
    list_alerts,
    list_alerts_cross_service,
    list_audit,
    list_views,
    lookup_asn_names,
    merge_audit_for_service,
    record_audit,
    replace_audit_for_service,
    replace_views_for_service,
    save_alert,
    save_view,
    toggle_alert,
    update_alert_last_triggered,
    upsert_asn_names,
    upsert_views_for_service,
)

# Source registry + usage telemetry + persistent slow-SQL history.
from backend.core.metadata.usage_log import (
    DEFAULT_METADATA_RETENTION,
    _query_usage_log_aggregate_rollup,
    clear_slow_queries,
    clear_usage_log,
    count_slow_queries,
    flush_slow_query_buffer,
    get_usage_logs,
    insert_slow_query,
    iter_usage_logs_chunks,
    list_slow_queries,
    log_synthetic_usage,
    log_usage_calls,
    purge_usage_log,
    reconcile_fastly_stats,
    register_source,
)


class _ShimModule(ModuleType):
    """Mirror writes for shared state into ``metadata.base``.

    ``_DATA_DIR`` / ``_SCHEMA`` etc. are owned by
    ``metadata.base``. Tests and ``state_sync`` use
    ``setattr(metadata, "_DATA_DIR", ...)`` (directly or via
    ``monkeypatch.setattr``) and expect the live ``get_con`` bindings to
    swap. Without this proxy, the write would only rebind the package's
    re-export, leaving ``base`` reading the un-patched value.
    """

    _MIRRORED_TO_BASE = frozenset(
        {
            "_DATA_DIR",
            "_SCHEMA",
            "_ingested_filenames_cache",
            "_ingested_filenames_cache_lock",
            "_FILE_DATE_RE",
            "_parse_file_date",
        }
    )

    def __setattr__(self, name: str, value) -> None:
        if name in self._MIRRORED_TO_BASE:
            setattr(_base, name, value)
        if name == "_DATA_DIR":
            setattr(_usage_log, name, value)
        super().__setattr__(name, value)


sys.modules[__name__].__class__ = _ShimModule
for _sub in ("alerts", "asn_cache", "views"):
    setattr(sys.modules[__name__], _sub, _state)
    sys.modules[f"{__name__}.{_sub}"] = _state
for _sub in ("usage_log_db", "slow_queries"):
    setattr(sys.modules[__name__], _sub, _usage_log)
    sys.modules[f"{__name__}.{_sub}"] = _usage_log
setattr(sys.modules[__name__], "quarantine", _ingest_log)
sys.modules[f"{__name__}.quarantine"] = _ingest_log
_backend_pkg = sys.modules.get("backend")
if _backend_pkg is not None and hasattr(_backend_pkg, "_MODULE_ALIASES"):
    _backend_pkg._MODULE_ALIASES.update(
        {
            "backend.core.query_attribution": "backend.core.request_telemetry",
            "backend.core.request_metrics": "backend.core.request_telemetry",
            "backend.request_metrics_observer": "backend.core.request_telemetry",
            "backend.core.metric_snapshots": "backend.cron.jobs.metadata",
            "backend.core.duckdb_recycle": "backend.cron.jobs.metadata",
            "backend.cron.jobs.duckdb_recycle": "backend.cron.jobs.metadata",
            "backend.core.metadata.slow_queries": "backend.core.metadata.usage_log",
            "backend.cron.decorators": "backend.cron.scheduler",
            "backend.cron.jobs.rum_commit": "backend.cron.jobs.rum_sync",
        }
    )


__all__ = [
    # Connection / schema (public)
    "db_path",
    "get_con",
    "get_con_readonly",
    "close_all_connections",
    "teardown",
    # Alerts
    "list_alerts",
    "list_alerts_cross_service",
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
    # Ingested files
    "get_ingested_filenames",
    "get_reclaimable_strand_filenames",
    "list_ingested_files",
    "get_ingested_files_status_summary",
    "get_log_accounting_counts",
    "get_storage_stats_window",
    "list_unbackfilled_fastly_edge_files",
    "get_latest_ingest_ts",
    "get_latest_reconciliation_ts",
    "insert_ingested_files",
    "record_in_flight",
    "clear_in_flight",
    "list_in_flight",
    "get_log_activity",
    "get_node_count_avg",
    "get_rum_beacons_avg_per_day",
    "get_rum_beacons_by_date",
    # Persistent slow-SQL history
    "clear_slow_queries",
    "flush_slow_query_buffer",
    "insert_slow_query",
    "list_slow_queries",
    "count_slow_queries",
    # Cron runs
    "start_cron_run",
    "log_cron_run",
    "finalize_cron_run_if_running",
    "update_cron_duration",
    "delete_cron_run",
    "purge_cron_runs",
    "record_scoring_audit",
    "list_scoring_audit",
    "get_cron_run_result",
    "get_cron_run_status",
    "get_cron_runs",
    "latest_cron_per_task",
    "reap_running_cron_runs",
    "cron_busy",
    "adaptive_stale_minutes",
    # ASN cache
    "lookup_asn_names",
    "upsert_asn_names",
    "asn_ints_for_search",
    # Sources
    "register_source",
    # Usage log
    "log_usage_calls",
    "log_synthetic_usage",
    "reconcile_fastly_stats",
    "purge_usage_log",
    "clear_usage_log",
    "get_usage_logs",
    "iter_usage_logs_chunks",
    "DEFAULT_METADATA_RETENTION",
    # Reconciliation / cleanup
    "get_metadata_storage_stats",
    "is_ingested_files_dedup_active",
    "cleanup_metadata",
    # Module-level state hooks used by tests + state_sync
    "_clear_ingested_filenames_cache",
    "_DATA_DIR",
    "_SCHEMA",
    "_ingested_filenames_cache",
    "_ingested_filenames_cache_lock",
    "_parse_file_date",
    "_FILE_DATE_RE",
    "_bootstrap_ingested_files_summary",
    "recompute_ingested_files_summary",
    "_query_usage_log_aggregate_rollup",
    "_STATS_TABLES",
    "_CLEANUP_TABLES",
    # Quarantine
    "get_quarantine_storage_total",
]
