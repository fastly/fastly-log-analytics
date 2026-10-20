"""Per-service least-privilege ClickHouse identities for analyst SQL.

Analysts are an adversary, and the console validates SQL with DuckDB's parser while ClickHouse runs it, so
name-based checks can only ever be a second line of defence. The first is the database itself: each service gets
its own ClickHouse user whose

* grants are ``SELECT`` on the fact tables and the publication table only (no other table, no ``system.*``
  beyond what every user can read, no table-function sources, no DDL or inserts),
* settings profile is ``readonly = 1`` with every resource limit the client sends capped by ``MAX``, so a query
  cannot raise its own memory, threads or timeout, and
* restrictive row policies pin every readable table to that service's ``service_id``, which no query text or
  ``SETTINGS`` clause can remove.

Credentials are derived from one deployment secret (``CLICKHOUSE_CONSOLE_SECRET``), so nothing per service has
to be stored. With no secret configured analysts get no console (fail closed).
"""

from __future__ import annotations

import dataclasses
import hashlib
import hmac
import logging
import os
import re
import threading
from collections.abc import Callable, Iterable
from typing import Any

from backend import config
from backend.core.clickhouse_client import ClickHouseClient

logger = logging.getLogger(__name__)

SCOPED_TABLES = ("request_facts", "rum_vitals_facts", "rum_error_facts", "high_scale_batch_publications")
PROFILE = "fla_console_ro"
MIN_SECRET_BYTES = 32
_DATABASE = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}")

# Mirrors the per-query settings ClickHouseClient._request always sends, plus the three the console adds. Any
# setting a read-only user may change must be listed CHANGEABLE_IN_READONLY (the HTTP-only wait_end_of_query and
# default_format are not settings and need no entry); resource limits are capped so they cannot be raised.
_PROFILE_SETTINGS = (
    "readonly = 1, "
    "additional_table_filters CHANGEABLE_IN_READONLY, "
    "max_result_rows = 10001 MAX 10001 CHANGEABLE_IN_READONLY, "
    "result_overflow_mode CHANGEABLE_IN_READONLY, "
    "max_execution_time = 30 MAX 300 CHANGEABLE_IN_READONLY, "
    "max_memory_usage = 1073741824 MAX 1073741824 CHANGEABLE_IN_READONLY, "
    "max_threads = 2 MAX 2 CHANGEABLE_IN_READONLY, "
    "max_bytes_before_external_group_by = 134217728 MAX 134217728 CHANGEABLE_IN_READONLY, "
    "max_bytes_before_external_sort = 134217728 MAX 134217728 CHANGEABLE_IN_READONLY, "
    "timeout_overflow_mode CHANGEABLE_IN_READONLY, "
    "timeout_before_checking_execution_speed CHANGEABLE_IN_READONLY, "
    "output_format_json_quote_64bit_integers CHANGEABLE_IN_READONLY, "
    "memory_overcommit_ratio_denominator CHANGEABLE_IN_READONLY, "
    "memory_overcommit_ratio_denominator_for_user CHANGEABLE_IN_READONLY"
)


def _literal(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def console_identity(service_id: str, secret: bytes) -> tuple[str, str]:
    """``(user, password)`` for ``service_id``; the password is an HMAC of the service under the deployment secret."""
    if len(secret) < MIN_SECRET_BYTES:
        raise ValueError(f"CLICKHOUSE_CONSOLE_SECRET must be at least {MIN_SECRET_BYTES} bytes")
    user = "fla_console_" + hashlib.sha256(service_id.encode()).hexdigest()[:16]
    password = hmac.new(secret, f"console:{service_id}".encode(), hashlib.sha256).hexdigest()
    return user, password


def provision_console_access(admin: Any, database: str, service_id: str, secret: bytes) -> str:
    """Idempotently create ``service_id``'s console identity and return its user name.

    Order matters: the row policy exists before any ``SELECT`` is granted, because a user with grants but no
    policy would see every service's rows. If any statement fails nothing broader than a grantless user remains.
    """
    if not _DATABASE.fullmatch(database):
        raise ValueError("ClickHouse database must be a simple identifier")
    user, password = console_identity(service_id, secret)
    db = f"`{database}`"
    tables = ", ".join(f"{db}.`{table}`" for table in SCOPED_TABLES)
    statements = [
        f"CREATE SETTINGS PROFILE IF NOT EXISTS {PROFILE} SETTINGS {_PROFILE_SETTINGS}",
        f"ALTER SETTINGS PROFILE {PROFILE} SETTINGS {_PROFILE_SETTINGS}",
        f"CREATE USER IF NOT EXISTS `{user}` IDENTIFIED WITH sha256_password BY {_literal(password)} "
        f"DEFAULT DATABASE {db} SETTINGS PROFILE '{PROFILE}'",
        f"ALTER USER `{user}` IDENTIFIED WITH sha256_password BY {_literal(password)}",
        f"CREATE ROW POLICY IF NOT EXISTS `{user}_scope` ON {tables} FOR SELECT "
        f"USING service_id = {_literal(service_id)} AS RESTRICTIVE TO `{user}`",
        *(f"GRANT SELECT ON {db}.`{table}` TO `{user}`" for table in SCOPED_TABLES),
    ]
    for statement in statements:
        admin.execute(statement)
    return user


def provision_configured_services(admin: Any, service_ids: Iterable[str]) -> tuple[str, ...]:
    """Provision every service at startup when a console secret is configured; failures are logged, not fatal."""
    secret = _env_secret()
    settings = config.load_clickhouse_config()
    if secret is None or settings is None:
        return ()
    done = []
    for service_id in service_ids:
        try:
            provision_console_access(admin, settings.database, service_id, secret)
            done.append(service_id)
        except Exception:
            logger.warning(
                "analyst console provisioning failed for %s; analysts get no console there", service_id, exc_info=True
            )
    return tuple(done)


def _env_secret() -> bytes | None:
    value = os.getenv("CLICKHOUSE_CONSOLE_SECRET", "").strip()
    return value.encode() if value else None


class ConsoleClients:
    """Lazy per-service ClickHouse clients that log in as each service's own console identity."""

    def __init__(
        self,
        *,
        settings: config.ClickHouseConfig | None,
        secret: bytes | None,
        client_factory: Callable[[config.ClickHouseConfig], Any] = ClickHouseClient,
    ) -> None:
        self._settings = settings
        self._secret = secret
        self._factory = client_factory
        self._clients: dict[str, Any] = {}
        self._lock = threading.Lock()

    def client_for(self, service_id: str) -> Any | None:
        """The service's console client, or None when no secret/ClickHouse is configured (analysts get no console)."""
        if self._secret is None or self._settings is None:
            return None
        with self._lock:
            existing = self._clients.get(service_id)
            if existing is not None:
                return existing
            user, password = console_identity(service_id, self._secret)
            client = self._factory(dataclasses.replace(self._settings, user=user, password=password))
            try:
                self._assert_isolated(client, service_id)
            except BaseException:
                client.close()
                raise
            self._clients[service_id] = client
            return client

    @staticmethod
    def _assert_isolated(client: Any, service_id: str) -> None:
        """Fail closed unless the identity cannot read another service's rows (a missing policy would fail open)."""
        rows = client.execute(
            f"SELECT count() AS n FROM request_facts WHERE service_id != {_literal(service_id)}",
        )
        if not rows or int(rows[0].get("n", 1)) != 0:
            raise RuntimeError(f"console identity for {service_id} failed the row isolation check")

    def close(self) -> None:
        with self._lock:
            clients, self._clients = list(self._clients.values()), {}
        for client in clients:
            client.close()


_manager: ConsoleClients | None = None
_manager_lock = threading.Lock()


def get_console_clients() -> ConsoleClients:
    """Process singleton built from the environment (CLICKHOUSE_* plus CLICKHOUSE_CONSOLE_SECRET)."""
    global _manager
    with _manager_lock:
        if _manager is None:
            _manager = ConsoleClients(settings=config.load_clickhouse_config(), secret=_env_secret())
        return _manager


def close_console_clients() -> None:
    global _manager
    with _manager_lock:
        manager, _manager = _manager, None
    if manager is not None:
        manager.close()
