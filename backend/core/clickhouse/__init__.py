"""Bounded, synchronous ClickHouse HTTP boundary (no serving-path fallback).

``execute`` accepts TRUSTED internal SQL, including DDL, using ClickHouse typed
placeholders: ``SELECT ... WHERE service_id = {service_id:String}``. Parameters
are values only, never identifiers. Do not pass user-authored SQL here.
``insert_rows`` accepts only the internal identifiers below. The later schema
module imports/extends these constants; this module must never import schema.
"""

from __future__ import annotations

import sys
import time
from typing import Any

import httpx
import structlog
from botocore.exceptions import ClientError
from opentelemetry.metrics import Observation
from psycopg import Connection
from psycopg_pool import ConnectionPool

from backend import config
from backend.core.clickhouse import client, constants, health, params
from backend.core.clickhouse.client import (
    ClickHouseClient,
    FosArtifacts,
    _client,
    _client_lock,
    _instrument,
    _record_operation,
    _require_client,
    _safe_count,
    _server_elapsed_ms,
    close_clickhouse_client,
    get_clickhouse_client,
    record_operation,
    record_publication,
)
from backend.core.clickhouse.constants import (
    _DDL_PASSWORD,
    _PARAM_ESCAPES,
    _PARAM_NAME,
    _QUERY_SETTINGS,
    ARTIFACT_SCHEMA,
    CLICKHOUSE_ALLOWED_TABLES,
    CLICKHOUSE_FACT_COLUMNS,
    CLICKHOUSE_FACT_DDL,
    CLICKHOUSE_FACT_TABLE,
    CLICKHOUSE_HIGH_SCALE_COLUMNS,
    CLICKHOUSE_HIGH_SCALE_TABLES,
    CLICKHOUSE_SCHEMA_VERSION,
    CLICKHOUSE_TABLE_COLUMNS,
    FACT_TYPES,
    MAX_ARTIFACT_BYTES,
    MAX_BATCH_ROWS,
    MAX_DATASET_ROWS,
    MAX_ROW_BYTES,
    PAYLOAD_COLUMNS,
    Artifact,
    ClickHouseError,
    Dataset,
    DurableBatch,
    PublicationResult,
    ReplayResult,
    _construct,
)
from backend.core.clickhouse.health import (
    _OBSERVER_CACHE,
    ClickHouseObserver,
    PgManifest,
    _ensure_manifest_symbols,
    _observer,
    _publish_batch,
    _record_schema,
    _target_rows,
    activate_generation,
    create_clickhouse_schema,
    full_rebuild,
    main,
    maintain_clickhouse_system_tables,
    publish_batch,
    replay_unpublished,
    require_postgres,
    start_clickhouse_observer,
    stop_clickhouse_observer,
    target_identity,
    verify_generation,
)
from backend.core.clickhouse.params import (
    _date_value,
    _json_value,
    _parameter,
    _redact_ddl_secrets,
    _validate_payload,
    build_batch_id,
    canonical_bytes,
    canonical_row,
    digest_rows,
    utc,
)
from backend.core.duckdb import _get_fos_client
from backend.core.metadata import base as pg_connection
from backend.core.metadata.base import CLICKHOUSE_CONTROL_DDL, get_pg_pool
from backend.core.request_telemetry import current_clickhouse_scope, get_meter, query_registry
from backend.utils.bounded_cache import BoundedTTLCache, CacheRegistry
from backend.utils.module_shim import install_shim_module
from backend.utils.telemetry import _QUERIES, ensure_utc, get_queries, parse_iso_utc, record_call

logger = structlog.get_logger(__name__)


def __getattr__(name: str) -> Any:
    if name == "readiness":
        return None
    if name in ("Artifact", "Dataset", "PgManifest"):
        _ensure_manifest_symbols()
        return globals()[name]
    raise AttributeError(name)


_self = sys.modules[__name__]
_SYNC_MODULES = (_self, constants, params, client, health)

_SKIP_ATTRS = frozenset(
    {
        "__name__",
        "__doc__",
        "__package__",
        "__loader__",
        "__spec__",
        "__path__",
        "__file__",
        "__cached__",
        "__builtins__",
        "__all__",
        "__class__",
        "constants",
        "params",
        "client",
        "health",
        "_self",
        "_SYNC_MODULES",
        "_SKIP_ATTRS",
        "_ShimModule",
    }
)

for _attr_name, _attr_val in list(_self.__dict__.items()):
    if _attr_name not in _SKIP_ATTRS:
        for _mod in (constants, params, client, health):
            _mod.__dict__[_attr_name] = _attr_val

_ShimModule = install_shim_module(__name__, _SYNC_MODULES, skip_attrs=_SKIP_ATTRS)


__all__ = [
    "ARTIFACT_SCHEMA",
    "Artifact",
    "BoundedTTLCache",
    "CLICKHOUSE_ALLOWED_TABLES",
    "CLICKHOUSE_CONTROL_DDL",
    "CLICKHOUSE_FACT_COLUMNS",
    "CLICKHOUSE_FACT_DDL",
    "CLICKHOUSE_FACT_TABLE",
    "CLICKHOUSE_HIGH_SCALE_COLUMNS",
    "CLICKHOUSE_HIGH_SCALE_TABLES",
    "CLICKHOUSE_SCHEMA_VERSION",
    "CLICKHOUSE_TABLE_COLUMNS",
    "CacheRegistry",
    "ClickHouseClient",
    "ClickHouseError",
    "ClickHouseObserver",
    "ClientError",
    "Connection",
    "ConnectionPool",
    "Dataset",
    "DurableBatch",
    "FACT_TYPES",
    "FosArtifacts",
    "MAX_ARTIFACT_BYTES",
    "MAX_BATCH_ROWS",
    "MAX_DATASET_ROWS",
    "MAX_ROW_BYTES",
    "Observation",
    "PAYLOAD_COLUMNS",
    "PgManifest",
    "PublicationResult",
    "ReplayResult",
    "_DDL_PASSWORD",
    "_OBSERVER_CACHE",
    "_PARAM_ESCAPES",
    "_PARAM_NAME",
    "_QUERIES",
    "_QUERY_SETTINGS",
    "_client",
    "_client_lock",
    "_construct",
    "_date_value",
    "_ensure_manifest_symbols",
    "_get_fos_client",
    "_instrument",
    "_json_value",
    "_observer",
    "_parameter",
    "_publish_batch",
    "_record_operation",
    "_record_schema",
    "_redact_ddl_secrets",
    "_require_client",
    "_safe_count",
    "_server_elapsed_ms",
    "_target_rows",
    "_validate_payload",
    "activate_generation",
    "build_batch_id",
    "canonical_bytes",
    "canonical_row",
    "close_clickhouse_client",
    "config",
    "create_clickhouse_schema",
    "current_clickhouse_scope",
    "digest_rows",
    "ensure_utc",
    "full_rebuild",
    "get_clickhouse_client",
    "get_meter",
    "get_pg_pool",
    "get_queries",
    "httpx",
    "logger",
    "main",
    "maintain_clickhouse_system_tables",
    "parse_iso_utc",
    "pg_connection",
    "publish_batch",
    "query_registry",
    "record_call",
    "record_operation",
    "record_publication",
    "replay_unpublished",
    "require_postgres",
    "start_clickhouse_observer",
    "stop_clickhouse_observer",
    "target_identity",
    "time",
    "utc",
    "verify_generation",
]

if __name__ == "__main__":
    main()
