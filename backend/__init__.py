"""Backend package initialization with zero-eager-import module aliasing.

Installs a lightweight ``sys.meta_path`` finder/loader that maps consolidated
submodules to their folded target modules on first import without importing
any backend submodule at package initialization time (keeping grimp /
lint-imports dependency graph clean).
"""

from __future__ import annotations

import importlib
import importlib.abc
import importlib.machinery
import importlib.util
import os
import sys
import types
from collections.abc import Sequence

_BACKEND_DIR = os.path.dirname(os.path.abspath(__file__))

_MODULE_ALIASES: dict[str, str] = {
    # Round 15 Group 1 — Backend root publishers & Celery glue
    "backend.celery_queue": "backend.celery_app",
    "backend._in_process_publisher": "backend.celery_app",
    "backend.sync_status_publisher": "backend.celery_app",
    "backend.celery_status_sampler": "backend.celery_app",
    "backend.cron_runs_publisher": "backend.cron.scheduler",
    "backend.sync_status_snapshot": "backend.cron.scheduler",
    "backend.request_metrics_observer": "backend.core.request_telemetry",
    # Round 15 Group 2 — backend/core/
    "backend.core.clickhouse_schema": "backend.core.clickhouse",
    "backend.core.clickhouse_observer": "backend.core.clickhouse",
    "backend.core.clickhouse_metrics": "backend.core.clickhouse",
    "backend.core.session_token": "backend.core.request_context",
    "backend.core.high_scale_sections": "backend.core.request_telemetry",
    "backend.core.metric_expressions": "backend.core.log_fields",
    # Round 15 Group 3 — backend/high_scale/
    "backend.high_scale.source_discovery": "backend.high_scale.ingest_controller",
    "backend.high_scale.query_router": "backend.high_scale.query_service",
    "backend.high_scale.exports": "backend.high_scale.archive",
    "backend.high_scale.archive_writer": "backend.high_scale.archive",
    "backend.high_scale.performance_projection": "backend.high_scale.query_service",
    # Round 15 Group 4 — backend/models/, backend/utils/
    "backend.models.rum": "backend.models.analytics",
    "backend.models._base": "backend.models.common",
    "backend.models.high_scale": "backend.models.admin",
    "backend.models.admin_status": "backend.models.admin",
    "backend.models.sessions": "backend.models.analytics",
    "backend.utils.cache_registry": "backend.utils.bounded_cache",
    "backend.utils.atomic_write": "backend.utils.telemetry",
    "backend.utils.fastly_auth": "backend.core.log_fields",
    "backend.utils.active_requests": "backend.utils.telemetry",
    "backend.utils.sqlite_profiler": "backend.utils.telemetry",
    # Round 16 — Repositories & Routers
    "backend.repositories.audit": "backend.repositories.assets",
    "backend.repositories.insights_helpers": "backend.repositories.insights",
    "backend.routers.debug": "backend.routers.bootstrap",
    "backend.routers.sessions": "backend.routers.session_scoring",
    "backend.routers.share_sso": "backend.routers.share_admin",
    "backend.routers.share": "backend.routers.share_admin",
    "backend.routers.system": "backend.routers.bootstrap",
    "backend.routers.health": "backend.routers.bootstrap",
    # Round 20 — Backend <250-Line Module Consolidation
    "backend.utils.error_redaction": "backend.utils.telemetry",
    "backend.utils.valkey_publisher": "backend.utils.telemetry",
    "backend.utils.vcl_safety": "backend.core.log_fields",
    "backend.system_metrics_sampler": "backend.routers.admin.health",
    "backend.scoring.scorer": "backend.scoring",
    "backend.scoring.normalize": "backend.scoring",
    "backend.core.fastly.mock_fixtures": "backend.core.fastly.client",
    "backend.core.rollups.perf_latency": "backend.core.rollups.origin_dims",
    "backend.core.rollups.perf_dims": "backend.core.rollups.origin_dims",
    "backend.core.quarantine": "backend.core.metadata.ingest_log",
    "backend.core.metadata.quarantine": "backend.core.metadata.ingest_log",
    "backend.provision.declarative.diff": "backend.provision.declarative.reconciler",
    "backend.core.realtime.poller": "backend.core.realtime",
    "backend.high_scale.origin_projection": "backend.high_scale.query_service",
    "backend.high_scale.replay": "backend.high_scale.archive",
    "backend.models.security": "backend.models.analytics",
    # Round 25 — Backend <300-Line Module Consolidation
    "backend.core.query_attribution": "backend.core.request_telemetry",
    "backend.core.request_metrics": "backend.core.request_telemetry",
    "backend.core.metric_snapshots": "backend.cron.jobs.metric_snapshots",
    "backend.core.duckdb_recycle": "backend.cron.jobs.duckdb_recycle",
    "backend.core.metadata.slow_queries": "backend.core.metadata.usage_log",
    "backend.cron.decorators": "backend.cron.scheduler",
    "backend.scoring.labels": "backend.scoring",
    "backend.scoring.fixtures": "backend.scoring",
    "backend.scoring.matrix": "backend.scoring",
    "backend.utils.vcl_validator": "backend.core.log_fields",
    "backend.high_scale.aggregate_batch_adapter": "backend.high_scale._common",
    "backend.high_scale.registry_bootstrap": "backend.high_scale.ingest_controller",
    "backend.models.admin_actions": "backend.models.analytics",
    "backend.models.origin": "backend.models.analytics",
    "backend.repositories.usage": "backend.repositories.assets",
    # Rounds 31, 32 & 34 — Backend <400-Line Module Consolidation
    "backend.core.oauth.registry": "backend.core.oauth",
    "backend.core.oauth.client": "backend.core.oauth",
    "backend.core.oauth.flow_state": "backend.core.oauth",
    "backend.core.share_db.validation": "backend.core.share_db",
    "backend.core.share_db.invites": "backend.core.share_db",
    "backend.routers.share_auth": "backend.routers.share_admin",
    "backend.routers.remote_share": "backend.routers.share_admin",
    "backend.utils.telemetry_response_middleware": "backend.utils.telemetry",
    "backend.utils.structlog_config": "backend.utils.telemetry",
    "backend.utils.usage_logger": "backend.utils.telemetry",
    "backend.utils.log_throttle": "backend.utils.telemetry",
    "backend.utils.ngwaf": "backend.core.rollups.security_dims",
    "backend.core.rollups.ngwaf_bots": "backend.core.rollups.security_dims",
    "backend.core.rollups.hour_bundles": "backend.core.rollups.day_bundles",
    "backend.core.rollups.time_series": "backend.core.rollups.recompute",
    "backend.core.rollups.overview": "backend.core.rollups.recompute",
    "backend.core.rollups.sessions": "backend.core.rollups.recompute",
    "backend.core._log_fields_data": "backend.core.log_fields",
    "backend.state_sync": "backend.core.metadata.base",
    "backend.scoring.evaluate": "backend.scoring",
    "backend.high_scale.assets": "backend.high_scale.recovery",
    "backend.high_scale.console_access": "backend.high_scale.postgres_control",
    "backend.high_scale.deletion": "backend.high_scale.postgres_control",
    "backend.high_scale.admin_plane": "backend.high_scale.postgres_control",
    "backend.high_scale.decoder": "backend.high_scale.query",
    "backend.high_scale.clickhouse": "backend.high_scale.query_service",
    "backend.high_scale.rum_recovery": "backend.high_scale.rum",
    "backend.high_scale.rum_ingest": "backend.high_scale.ingest_controller",
    "backend.cron.lifecycle": "backend.cron.scheduler",
    "backend.routers.admin_queries": "backend.routers.admin.admin_queries",
    "backend.models.network": "backend.models.analytics",
    "backend.models.provision": "backend.models.common",
    "backend.models.service": "backend.models.common",
    "backend.provision.utils": "backend.provision.cli",
    "backend.provision.log_paths": "backend.provision.cli",
    "backend.provision.draft_version": "backend.provision.cli",
    "backend.provision.declarative.iac_reconciler": "backend.provision.declarative.reconciler",
    "backend.core.fastly.rum_provisioning": "backend.core.fastly.client",
    "backend.core.fastly.utils": "backend.core.fastly.client",
    "backend.core.fastly.api": "backend.core.fastly.client",
    "backend.core.fastly.service": "backend.core.fastly.client",
    "backend.routers.session_scoring_admin": "backend.routers.session_scoring",
    "backend.core._duckdb_status": "backend.core.duckdb",
    # Rounds 39 & 40 — Backend <550-Line Module Consolidation
    "backend.core.metadata.pg_schema": "backend.core.metadata.base",
    "backend.core.metadata.reconciliation": "backend.core.metadata.base",
    "backend.core.storage.manifest": "backend.core.storage._core",
    "backend.cron_progress": "backend.cron.scheduler",
    "backend.cron.jobs.metric_snapshot": "backend.cron.jobs.metric_snapshots",
    "backend.celery_status": "backend.celery_app",
    "backend.core.rollups.partial_hour": "backend.core.rollups.day_bundles",
    "backend.core.rollups.wellknown_bots": "backend.core.rollups.security_dims",
    "backend.models.share_admin": "backend.models.admin",
    "backend.models.performance": "backend.models.analytics",
    "backend.routers.share_oauth": "backend.routers.share_admin",
    "backend.core.clickhouse_rows": "backend.core.clickhouse",
    "backend.core.metadata.ingest_ledger": "backend.core.metadata.usage_log",
    "backend.repositories.insights._rollups": "backend.repositories.insights.definitions",
    "backend.repositories.insights.filters": "backend.repositories.insights.definitions",
    "backend.high_scale.archive_publication": "backend.high_scale.archive",
    "backend.high_scale.archive_models": "backend.high_scale.archive",
    "backend.high_scale.models": "backend.high_scale.archive",
    "backend.high_scale.disk_sampler": "backend.high_scale.archive",
    "backend.high_scale.retention_watermark": "backend.high_scale.archive",
    "backend.high_scale.aggregate_batch": "backend.high_scale.ingest_controller",
    "backend.core.clickhouse_manifest": "backend.core.clickhouse",
    "backend.provision.session_scoring_vcl": "backend.provision.session_scoring_orchestrator",
    "backend.provision.declarative.state": "backend.provision.declarative.reconciler",
    "backend.provision.declarative.config_migration": "backend.provision.declarative.reconciler",
    # Round 49 — Backend <650-Line Module Consolidation
    "backend.repositories._sql.rum": "backend.repositories.rum",
    "backend.repositories._sql.security": "backend.repositories.security",
    "backend.deps": "backend.core.request_context",
    "backend.core.query_instrumentation": "backend.core.request_telemetry",
    "backend.core.clickhouse_publication": "backend.core.clickhouse",
    "backend.core.storage._ducklake": "backend.core.storage._core",
    "backend.provision.declarative.fastly_integration": "backend.provision.declarative.reconciler",
    "backend.provision.declarative.generators": "backend.provision.declarative.reconciler",
    "backend.provision.session_scoring_setup": "backend.provision.session_scoring_orchestrator",
    "backend.core.rollups.network": "backend.core.rollups.origin_dims",
    "backend.core.rollups.rum": "backend.core.rollups.recompute",
    "backend.models.session_scoring": "backend.models.analytics",
    "backend.models.dashboard": "backend.models.analytics",
    "backend.repositories.alerts": "backend.repositories.assets",
    # Round 61 — Backend <850-Line Module Consolidation
    "backend.utils.sql_validator": "backend.core.duckdb",
    "backend.core.reset": "backend.core.duckdb",
    "backend.core.query_registry": "backend.core.request_telemetry",
    "backend.core.storage.view": "backend.core.storage._core",
    "backend.core.clickhouse_client": "backend.core.clickhouse",
    "backend.utils.rdns_cache": "backend.utils.bot_sources",
    "backend.core.field_registry": "backend.core.log_fields",
    "backend.utils.vcl_utils": "backend.core.log_fields",
    "backend.core.metadata.pg_connection": "backend.core.metadata.base",
    "backend.core.realtime.transform": "backend.core.realtime",
    "backend.utils.tunnel.manager": "backend.utils.tunnel",
    "backend.models.insights": "backend.models.analytics",
    "backend.models.services": "backend.models.common",
    "backend.scoring.features": "backend.scoring",
    "backend.repositories.session_scoring": "backend.repositories.sessions",
    "backend.repositories.rollup_coverage": "backend.core.rollups.recompute",
    "backend.high_scale.orchestration": "backend.high_scale.ingest_controller",
    "backend.high_scale.historical_jobs": "backend.high_scale.archive",
    "backend.high_scale.publication": "backend.high_scale.archive",
    # Round 71 — Backend <1,100-Line Module Consolidation (Part A)
    "backend.core.metadata.state": "backend.core.metadata.base",
    "backend.core.metadata.cron_log": "backend.core.metadata.usage_log",
    "backend.core.local_compaction": "backend.core.storage._core",
    "backend.core.storage.buffer": "backend.core.storage._core",
    "backend.utils.telemetry_proxy": "backend.utils.telemetry",
    "backend.routers.high_scale": "backend.routers.admin.high_scale",
    "backend.high_scale.insight_queries": "backend.high_scale.insights",
}


class _AliasLoader(importlib.abc.MetaPathFinder, importlib.abc.ExecutionLoader):
    def __init__(self) -> None:
        self._canon_specs: dict[str, tuple[importlib.machinery.ModuleSpec | None, importlib.abc.Loader | None]] = {}

    def _resolve_origin(self, fullname: str) -> tuple[str, str | None]:
        target_name = _MODULE_ALIASES.get(fullname, fullname)
        target_mod = sys.modules.get(target_name)
        if target_mod is not None:
            mod_file = getattr(target_mod, "__file__", None)
            if mod_file:
                return target_name, mod_file
        if target_name.startswith("backend."):
            rel_parts = target_name.split(".")[1:]
            candidate_py = os.path.join(_BACKEND_DIR, *rel_parts) + ".py"
            if os.path.isfile(candidate_py):
                return target_name, candidate_py
            candidate_init = os.path.join(_BACKEND_DIR, *rel_parts, "__init__.py")
            if os.path.isfile(candidate_init):
                return target_name, candidate_init
        spec = importlib.machinery.PathFinder.find_spec(target_name)
        return target_name, (spec.origin if spec else None)

    def _has_real_file(self, fullname: str) -> bool:
        if not fullname.startswith("backend."):
            return False
        rel_parts = fullname.split(".")[1:]
        candidate_py = os.path.join(_BACKEND_DIR, *rel_parts) + ".py"
        candidate_init = os.path.join(_BACKEND_DIR, *rel_parts, "__init__.py")
        return os.path.isfile(candidate_py) or os.path.isfile(candidate_init)

    def find_spec(
        self,
        fullname: str,
        path: Sequence[str] | None = None,
        target: types.ModuleType | None = None,
    ) -> importlib.machinery.ModuleSpec | None:
        if self._has_real_file(fullname):
            _MODULE_ALIASES.pop(fullname, None)
            return None
        if fullname in _MODULE_ALIASES:
            _, origin = self._resolve_origin(fullname)
            return importlib.machinery.ModuleSpec(fullname, self, origin=origin)
        return None

    def is_package(self, fullname: str) -> bool:
        return False

    def get_filename(self, fullname: str) -> str:
        _, origin = self._resolve_origin(fullname)
        return origin or ""

    def get_source(self, fullname: str) -> str | None:
        target_name, origin = self._resolve_origin(fullname)
        if origin and origin.endswith(".py"):
            return importlib.machinery.SourceFileLoader(target_name, origin).get_source(target_name)
        return None

    def get_code(self, fullname: str) -> types.CodeType | None:
        target_name, origin = self._resolve_origin(fullname)
        if origin and origin.endswith(".py"):
            return importlib.machinery.SourceFileLoader(target_name, origin).get_code(target_name)
        return None

    def create_module(self, spec: importlib.machinery.ModuleSpec) -> types.ModuleType | None:
        target_name = _MODULE_ALIASES[spec.name]
        mod = importlib.import_module(target_name)
        if target_name not in self._canon_specs and not isinstance(getattr(mod, "__loader__", None), _AliasLoader):
            self._canon_specs[target_name] = (getattr(mod, "__spec__", None), getattr(mod, "__loader__", None))
        sys.modules[spec.name] = mod
        parent_name, _, child_name = spec.name.rpartition(".")
        parent_mod = sys.modules.get(parent_name)
        if parent_mod is not None:
            setattr(parent_mod, child_name, mod)
        return mod

    def exec_module(self, module: types.ModuleType) -> None:
        requested_name = module.__name__
        canon_name = _MODULE_ALIASES.get(requested_name, requested_name)
        module.__name__ = canon_name
        canon_spec, _canon_loader = self._canon_specs.get(canon_name, (None, None))
        if canon_spec is not None:
            canon_spec.loader = self
            module.__spec__ = canon_spec
        module.__loader__ = self
        for alias, target_name in list(_MODULE_ALIASES.items()):
            if target_name == canon_name:
                if self._has_real_file(alias):
                    _MODULE_ALIASES.pop(alias, None)
                    continue
                sys.modules[alias] = module
                parent_name, _, child_name = alias.rpartition(".")
                parent_mod = sys.modules.get(parent_name)
                if parent_mod is not None:
                    setattr(parent_mod, child_name, module)


if not any(isinstance(f, _AliasLoader) for f in sys.meta_path):
    sys.meta_path.insert(0, _AliasLoader())
