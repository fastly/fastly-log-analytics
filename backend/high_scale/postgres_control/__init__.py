"""Transactional Postgres control-plane state for the isolated high-scale slice."""

from __future__ import annotations

import contextlib
import dataclasses
import hashlib
import hmac
import json
import logging
import os
import re
import threading
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from psycopg_pool import ConnectionPool

from backend import config
from backend.core.clickhouse import ClickHouseClient
from backend.core.metadata.base import get_pg_pool
from backend.core.share_db import IP_FAMILY_KEYS, SESSION_ID_KEYS
from backend.high_scale.archive import (
    ArchiveArtifact,
    ArchiveManifest,
    ArchivePublication,
    ArchiveSourceEntry,
    ArchiveSourceObject,
    ArchiveState,
    ObjectStore,
    validate_archive_transition,
)
from backend.high_scale.ingest_controller import INITIAL_SOURCE_CURSOR, RUM_SOURCE_DOMAINS

logger = logging.getLogger("backend.high_scale.postgres_control")
import sys

from backend.high_scale.postgres_control import console_access as _console_access_mod
from backend.high_scale.postgres_control import control_plane as _control_plane_mod
from backend.high_scale.postgres_control import deletion as _deletion_mod
from backend.high_scale.postgres_control.console_access import (
    _DATABASE,
    _PROFILE_SETTINGS,
    FACT_DOMAINS,
    MASKED_KEYS,
    MIN_DISTINCT_SECRET_BYTES,
    MIN_SECRET_BYTES,
    PROFILE,
    PUBLICATIONS_TABLE,
    REDACTED,
    ConsoleAccess,
    ConsoleClients,
    _env_secret,
    _literal,
    _manager,
    _manager_lock,
    _mode,
    _redaction,
    _service_hash,
    _view_select,
    close_console_clients,
    console_identity,
    console_tables,
    get_console_clients,
    provision_configured_services,
    provision_console_access,
    provisioning_client,
)
from backend.high_scale.postgres_control.control_plane import (
    SCHEMA_DDL,
    ArchiveManifestRecord,
    OwnerEpochRecord,
    PostgresControlPlane,
    SourceClaim,
    SourceObjectRecord,
    _archive_from_row,
    _check_epoch,
    _check_epoch_value,
    _fetch_required,
    _observed_utc,
    _owner_from_row,
    _parse_iso_or_dt,
    _require_text,
    _require_texts,
    _row_value,
    _row_value_optional,
    _source_from_row,
    _source_object_id,
    _upsert_domain_cursor,
)
from backend.high_scale.postgres_control.deletion import (
    DeletionAuthorization,
    DeletionController,
    DeletionLedger,
    DeletionSweepResult,
    HighScaleArchiveExpirer,
    HighScaleDeletionSweeper,
    NoReplayLease,
    OwnerRecord,
    OwnershipChecker,
    PostgresDeletionLedger,
    ReplayLeaseChecker,
    _configured_domain_retention,
)
from backend.utils.module_shim import install_shim_module

_SUBMODULES = (_control_plane_mod, _console_access_mod, _deletion_mod)
_ShimModule = install_shim_module(__name__, _SUBMODULES)

__all__ = [
    "contextlib",
    "dataclasses",
    "hashlib",
    "hmac",
    "json",
    "logging",
    "os",
    "re",
    "threading",
    "Callable",
    "Iterable",
    "Iterator",
    "Mapping",
    "Sequence",
    "ThreadPoolExecutor",
    "contextmanager",
    "dataclass",
    "UTC",
    "datetime",
    "timedelta",
    "Any",
    "Protocol",
    "ConnectionPool",
    "config",
    "ClickHouseClient",
    "get_pg_pool",
    "IP_FAMILY_KEYS",
    "SESSION_ID_KEYS",
    "ArchiveArtifact",
    "ArchiveManifest",
    "ArchivePublication",
    "ArchiveSourceEntry",
    "ArchiveSourceObject",
    "ArchiveState",
    "ObjectStore",
    "validate_archive_transition",
    "INITIAL_SOURCE_CURSOR",
    "RUM_SOURCE_DOMAINS",
    "logger",
    "sys",
    "FACT_DOMAINS",
    "PUBLICATIONS_TABLE",
    "PROFILE",
    "MIN_SECRET_BYTES",
    "MIN_DISTINCT_SECRET_BYTES",
    "REDACTED",
    "MASKED_KEYS",
    "_DATABASE",
    "_PROFILE_SETTINGS",
    "_literal",
    "_service_hash",
    "_mode",
    "console_identity",
    "console_tables",
    "_redaction",
    "_view_select",
    "provision_console_access",
    "provisioning_client",
    "provision_configured_services",
    "_env_secret",
    "ConsoleAccess",
    "ConsoleClients",
    "_manager",
    "_manager_lock",
    "get_console_clients",
    "close_console_clients",
    "SCHEMA_DDL",
    "OwnerEpochRecord",
    "SourceObjectRecord",
    "SourceClaim",
    "ArchiveManifestRecord",
    "PostgresControlPlane",
    "_fetch_required",
    "_upsert_domain_cursor",
    "_owner_from_row",
    "_source_from_row",
    "_parse_iso_or_dt",
    "_archive_from_row",
    "_row_value",
    "_row_value_optional",
    "_check_epoch",
    "_check_epoch_value",
    "_source_object_id",
    "_require_text",
    "_require_texts",
    "_observed_utc",
    "DeletionAuthorization",
    "ReplayLeaseChecker",
    "NoReplayLease",
    "OwnerRecord",
    "OwnershipChecker",
    "DeletionLedger",
    "DeletionController",
    "PostgresDeletionLedger",
    "DeletionSweepResult",
    "HighScaleArchiveExpirer",
    "_configured_domain_retention",
    "HighScaleDeletionSweeper",
]
