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
import sys
import types
from collections.abc import Sequence

_MODULE_ALIASES: dict[str, str] = {
    # Round 15 Group 1 — Backend root publishers & Celery glue
    "backend.celery_queue": "backend.celery_app",
    "backend._in_process_publisher": "backend.celery_app",
    "backend.sync_status_publisher": "backend.celery_app",
    "backend.celery_status_sampler": "backend.celery_status",
    "backend.cron_runs_publisher": "backend.cron_progress",
    "backend.sync_status_snapshot": "backend.cron_progress",
    "backend.request_metrics_observer": "backend.core.request_metrics",
    # Round 15 Group 2 — backend/core/
    "backend.core.clickhouse_schema": "backend.core.clickhouse_publication",
    "backend.core.clickhouse_observer": "backend.core.clickhouse_client",
    "backend.core.clickhouse_metrics": "backend.core.clickhouse_client",
    "backend.core.session_token": "backend.core.request_context",
    "backend.core.high_scale_sections": "backend.core.request_telemetry",
    "backend.core.metric_expressions": "backend.core.field_registry",
    # Round 15 Group 3 — backend/high_scale/
    "backend.high_scale.source_discovery": "backend.high_scale.orchestration",
    "backend.high_scale.query_router": "backend.high_scale.query_service",
    "backend.high_scale.exports": "backend.high_scale.historical_jobs",
    "backend.high_scale.archive_writer": "backend.high_scale.archive_publication",
    "backend.high_scale.performance_projection": "backend.high_scale.performance",
    # Round 15 Group 4 — backend/models/, backend/services/, backend/utils/
    "backend.models.rum": "backend.models.performance",
    "backend.models._base": "backend.models.common",
    "backend.models.high_scale": "backend.models.admin",
    "backend.models.admin_status": "backend.models.admin",
    "backend.models.sessions": "backend.models.dashboard",
    "backend.services.service_manager": "backend.utils.router_utils",
    "backend.utils.pop_utils": "backend.utils.countries",
    "backend.utils.dir_size": "backend.utils.router_utils",
    "backend.utils.sse": "backend.utils.router_utils",
    "backend.utils.error_utils": "backend.utils.router_utils",
    "backend.utils.pii_masking": "backend.utils.router_utils",
    "backend.utils.rate_limit": "backend.utils.remote_access",
    "backend.utils.auth": "backend.utils.remote_access",
    "backend.utils.bounded_cache": "backend.utils.time_window",
    "backend.utils.cache_registry": "backend.utils.time_window",
    "backend.utils.atomic_write": "backend.utils.structlog_config",
    "backend.utils.fastly_auth": "backend.utils.vcl_utils",
    "backend.utils.active_requests": "backend.utils.telemetry_response_middleware",
    "backend.utils.sqlite_profiler": "backend.utils.telemetry",
    # Round 16 — Repositories & Routers
    "backend.repositories.audit": "backend.repositories.alerts",
    "backend.repositories.insights_helpers": "backend.repositories.insights",
    "backend.routers.audit": "backend.routers.alerts",
    "backend.routers.debug": "backend.routers.bootstrap",
    "backend.routers.insights": "backend.routers.dashboard",
    "backend.routers.sessions": "backend.routers.session_scoring",
    "backend.routers.share_sso": "backend.routers.share",
    "backend.routers.system": "backend.routers.health",
    "backend.routers.web_vitals": "backend.routers.rum",
    # Round 20 — Backend <250-Line Module Consolidation
    "backend.utils.error_redaction": "backend.utils.structlog_config",
    "backend.utils.valkey_publisher": "backend.utils.telemetry",
    "backend.utils.vcl_safety": "backend.utils.vcl_utils",
    "backend.system_metrics_sampler": "backend.routers.admin.health",
    "backend.routers.admin.sync_status": "backend.routers.admin.health",
    "backend.routers.admin._helpers": "backend.routers.admin.high_scale",
    "backend.routers.admin._router": "backend.routers.admin.high_scale",
    "backend.routers.admin.clickhouse": "backend.routers.admin.high_scale",
    "backend.routers.admin.events": "backend.routers.admin.high_scale",
    "backend.scoring.scorer": "backend.scoring.features",
    "backend.scoring.normalize": "backend.scoring.features",
    "backend.cron.jobs._common": "backend.cron.jobs.sync",
    "backend.cron.jobs.commit": "backend.cron.jobs.sync",
    "backend.cron.jobs.rum_ledger": "backend.cron.jobs.rum_sync",
    "backend.core.fastly.mock_fixtures": "backend.core.fastly.client",
    "backend.core.rollups.perf_latency": "backend.core.rollups.origin_dims",
    "backend.core.rollups.perf_dims": "backend.core.rollups.origin_dims",
    "backend.core.quarantine": "backend.core.metadata.ingest_log",
    "backend.core.metadata.quarantine": "backend.core.metadata.ingest_log",
    "backend.provision.declarative.diff": "backend.provision.declarative.reconciler",
    "backend.core.realtime.poller": "backend.core.realtime.transform",
    "backend.high_scale.origin_projection": "backend.high_scale.origin",
    "backend.high_scale.replay": "backend.high_scale.historical_jobs",
    "backend.models.security": "backend.models.dashboard",
    "backend.routers.security": "backend.routers.network",
}


class _AliasLoader(importlib.abc.MetaPathFinder, importlib.abc.Loader):
    def find_spec(
        self,
        fullname: str,
        path: Sequence[str] | None = None,
        target: types.ModuleType | None = None,
    ) -> importlib.machinery.ModuleSpec | None:
        if fullname == "backend.services":
            return importlib.machinery.ModuleSpec(fullname, self, is_package=True)
        if fullname in _MODULE_ALIASES:
            return importlib.machinery.ModuleSpec(fullname, self)
        return None

    def create_module(self, spec: importlib.machinery.ModuleSpec) -> types.ModuleType | None:
        if spec.name == "backend.services":
            mod = types.ModuleType("backend.services")
            mod.__path__ = []
            return mod
        target_name = _MODULE_ALIASES[spec.name]
        mod = importlib.import_module(target_name)
        sys.modules[spec.name] = mod
        parent_name, _, child_name = spec.name.rpartition(".")
        parent_mod = sys.modules.get(parent_name)
        if parent_mod is not None:
            setattr(parent_mod, child_name, mod)
        return mod

    def exec_module(self, module: types.ModuleType) -> None:
        if module.__name__ == "backend.services":
            target = importlib.import_module("backend.utils.router_utils")
            module.service_manager = target  # type: ignore[attr-defined]
            module.delete_service_Cascade = target.delete_service_Cascade  # type: ignore[attr-defined]
            sys.modules["backend.services"] = module
            sys.modules["backend.services.service_manager"] = target
            parent_mod = sys.modules.get("backend")
            if parent_mod is not None:
                setattr(parent_mod, "services", module)
            return
        for alias, target_name in _MODULE_ALIASES.items():
            if module.__name__ == target_name:
                sys.modules[alias] = module
                parent_name, _, child_name = alias.rpartition(".")
                parent_mod = sys.modules.get(parent_name)
                if parent_mod is not None:
                    setattr(parent_mod, child_name, module)


if not any(isinstance(f, _AliasLoader) for f in sys.meta_path):
    sys.meta_path.insert(0, _AliasLoader())
