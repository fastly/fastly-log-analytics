"""Per-service, per-mask-mode least-privilege ClickHouse identities for analyst SQL.

Analysts are an adversary, and the console validates SQL with DuckDB's parser while ClickHouse runs it, so
name-based checks can only ever be a second line of defence. The first is the database itself. Each service gets
two ClickHouse users, ``masked`` (for ``mask_ips`` invites) and ``raw``, and each user

* can ``SELECT`` only from its own three **views** (``request_facts``, ``rum_vitals_facts``, ``rum_error_facts``)
  and nothing else: no base table, no ``system.*`` beyond what every user can read, no table-function sources, no
  DDL or inserts,
* sees only that service's visible batches, because the view fixes ``service_id`` and the publication fence,
* in the masked view reads ``'[redacted]'`` wherever the data holds a client IP or session identifier (the same
  columns and map keys the standard console redacts), so no string function can recover them: the data never
  leaves the database unmasked, and the masked user has no grant on the raw view or the base table, and
* runs under ``readonly = 1`` with every resource limit the client sends capped by ``MAX``.

The views execute with the definer's rights, so the identity never needs access to the base tables or to the
publication table. Credentials are derived from one deployment secret (``CLICKHOUSE_CONSOLE_SECRET``), so nothing
per service has to be stored. With no secret configured analysts get no console (fail closed).
"""

from __future__ import annotations

import contextlib
import dataclasses
import hashlib
import hmac
import logging
import os
import re
import threading
from collections.abc import Callable, Iterable, Iterator, Mapping
from typing import Any

from backend import config
from backend.core.clickhouse_client import ClickHouseClient
from backend.core.share_db.validation import IP_FAMILY_KEYS, SESSION_ID_KEYS

logger = logging.getLogger(__name__)

# canonical fact table -> publication domain fenced by its view
FACT_DOMAINS = {"request_facts": "request", "rum_vitals_facts": "rum_vitals", "rum_error_facts": "rum_errors"}
PUBLICATIONS_TABLE = "high_scale_batch_publications"
PROFILE = "fla_console_ro"
MIN_SECRET_BYTES = 32
MIN_DISTINCT_SECRET_BYTES = 8
REDACTED = "[redacted]"
# Per-client identifiers redacted for a mask_ips analyst: the standard console's families, the RUM client id, the
# CMCD session id (as stable a correlator as cookie_session) and the client-IP request headers a service may log.
MASKED_KEYS = tuple(
    sorted(
        IP_FAMILY_KEYS | SESSION_ID_KEYS | {"client_id", "sid", "x-forwarded-for", "true-client-ip", "fastly-client-ip"}
    )
)
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
    "memory_overcommit_ratio_denominator_for_user CHANGEABLE_IN_READONLY, "
    # Ceilings the client never sends, so they apply as non-changeable defaults: a user's concurrent queries
    # share 2 GiB, and a single result is cut off at 64 MiB (wait_end_of_query buffers it in the backend).
    "max_memory_usage_for_user = 2147483648 MAX 2147483648, "
    "max_result_bytes = 67108864"
)


def _literal(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _service_hash(service_id: str) -> str:
    return hashlib.sha256(service_id.encode()).hexdigest()[:16]


def _mode(masked: bool) -> str:
    return "m" if masked else "r"


def console_identity(service_id: str, secret: bytes, masked: bool = True) -> tuple[str, str]:
    """``(user, password)`` for ``service_id`` in the given mask mode; the password is an HMAC under the secret."""
    if len(secret) < MIN_SECRET_BYTES:
        raise ValueError(f"CLICKHOUSE_CONSOLE_SECRET must be at least {MIN_SECRET_BYTES} bytes")
    if len(set(secret)) < MIN_DISTINCT_SECRET_BYTES:
        raise ValueError(f"CLICKHOUSE_CONSOLE_SECRET needs at least {MIN_DISTINCT_SECRET_BYTES} distinct bytes")
    user = f"fla_console_{_service_hash(service_id)}_{_mode(masked)}"
    password = hmac.new(secret, f"console:{service_id}:{_mode(masked)}".encode(), hashlib.sha256).hexdigest()
    return user, password


def console_tables(service_id: str, masked: bool) -> dict[str, str]:
    """Canonical fact table name -> the view this identity reads it through."""
    return {table: f"fla_cv_{_service_hash(service_id)}_{_mode(masked)}_{table}" for table in FACT_DOMAINS}


def _redaction(name: str, type_: str, keys: str) -> str | None:
    """The expression that redacts column ``name`` for a masked identity, or None to pass it through unchanged.

    Raises when a column holds identifiers the code cannot redact: a view must never pass such a column through,
    so provisioning fails (the service gets no analyst console) rather than leaking.
    """
    redacted = _literal(REDACTED)
    if type_.startswith("Map("):
        match = re.fullmatch(r"Map\((.+?),\s*(.+)\)", type_)
        if not match or "String" not in match.group(1) or "String" not in match.group(2):
            raise RuntimeError(f"cannot redact column {name} of type {type_}")
        return (
            f"mapApply((k, v) -> (k, if(lower(k) IN ({keys}) AND NOT (isNull(v) OR v = ''), {redacted}, v)), "
            f"`{name}`) AS `{name}`"
        )
    if name.lower() in MASKED_KEYS:
        if "String" not in type_:
            raise RuntimeError(f"cannot redact column {name} of type {type_}")
        return f"if(isNull(`{name}`) OR `{name}` = '', `{name}`, {redacted}) AS `{name}`"
    return None


def _view_select(database: str, table: str, columns: list[tuple[str, str]], service_id: str, masked: bool) -> str:
    sid = _literal(service_id)
    fence = (
        f"batch_id IN (SELECT batch_id FROM `{database}`.`{PUBLICATIONS_TABLE}` FINAL "
        f"WHERE service_id = {sid} AND domain = '{FACT_DOMAINS[table]}' AND publication_state = 'visible')"
    )
    source = f"FROM `{database}`.`{table}` WHERE service_id = {sid} AND publication_state = 'visible' AND {fence}"
    if not masked:
        return f"SELECT * {source}"
    # Explicit column list, never `*`: a column added to the base table later must be invisible to a masked
    # identity until provisioning (which runs at every startup) has decided how to redact it.
    keys = ", ".join(_literal(key) for key in MASKED_KEYS)
    projection = ", ".join(_redaction(name, type_, keys) or f"`{name}`" for name, type_ in columns)
    return f"SELECT {projection} {source}"


def provision_console_access(admin: Any, database: str, service_id: str, secret: bytes) -> dict[str, str]:
    """Idempotently create ``service_id``'s masked and raw console identities; returns ``{mode: user}``.

    Order matters: a user is created with no grants, its views are created and verified, and only then is
    ``SELECT`` on those views (and nothing else) granted. If any statement fails nothing broader than a grantless
    user remains. Pre-view identities (base-table grants plus a row policy) are removed.
    """
    if not _DATABASE.fullmatch(database):
        raise ValueError("ClickHouse database must be a simple identifier")
    db = f"`{database}`"
    schemas = {
        table: [(str(row["name"]), str(row["type"])) for row in admin.execute(f"DESCRIBE TABLE {db}.`{table}`")]
        for table in FACT_DOMAINS
    }
    for table, columns in schemas.items():
        if not columns:
            raise RuntimeError(f"{table} has no columns; refusing to provision console access")

    legacy_user = f"fla_console_{_service_hash(service_id)}"
    legacy_tables = ", ".join(f"{db}.`{t}`" for t in (*FACT_DOMAINS, PUBLICATIONS_TABLE))
    admin.execute(f"DROP ROW POLICY IF EXISTS `{legacy_user}_scope` ON {legacy_tables}")
    admin.execute(f"DROP USER IF EXISTS `{legacy_user}`")

    admin.execute(f"CREATE SETTINGS PROFILE IF NOT EXISTS {PROFILE} SETTINGS {_PROFILE_SETTINGS}")
    admin.execute(f"ALTER SETTINGS PROFILE {PROFILE} SETTINGS {_PROFILE_SETTINGS}")

    users: dict[str, str] = {}
    for masked in (True, False):
        user, password = console_identity(service_id, secret, masked)
        views = console_tables(service_id, masked)
        admin.execute(
            f"CREATE USER IF NOT EXISTS `{user}` IDENTIFIED WITH sha256_password BY {_literal(password)} "
            f"DEFAULT DATABASE {db} SETTINGS PROFILE '{PROFILE}'"
        )
        # Always re-applied: rotates the password and re-attaches the profile on a pre-existing user.
        admin.execute(
            f"ALTER USER `{user}` IDENTIFIED WITH sha256_password BY {_literal(password)} SETTINGS PROFILE '{PROFILE}'"
        )
        for table, view in views.items():
            # DROP + CREATE rather than CREATE OR REPLACE: the latter works through a temporary table and needs
            # DROP TABLE, which would let the provisioner drop the data tables. The gap between the statements
            # fails closed (a grant on a missing view reads nothing) and a tampered view is still replaced.
            admin.execute(f"DROP VIEW IF EXISTS {db}.`{view}`")
            admin.execute(
                f"CREATE VIEW {db}.`{view}` DEFINER = CURRENT_USER SQL SECURITY DEFINER AS "
                + _view_select(database, table, schemas[table], service_id, masked)
            )
        present = {
            row.get("name")
            for row in admin.execute(
                f"SELECT name FROM system.tables WHERE database = {_literal(database)} AND engine = 'View' "
                f"AND name IN ({', '.join(_literal(v) for v in views.values())})"
            )
        }
        missing = set(views.values()) - present
        if missing:
            raise RuntimeError(f"console views missing for {service_id}: {sorted(missing)}; nothing was granted")
        # A pre-existing user of this name must not keep grants from an earlier life.
        admin.execute(f"REVOKE ALL ON *.* FROM `{user}`")
        for view in views.values():
            admin.execute(f"GRANT SELECT ON {db}.`{view}` TO `{user}`")
        users["masked" if masked else "raw"] = user
    return users


@contextlib.contextmanager
def provisioning_client(default_admin: Any) -> Iterator[Any]:
    """The client to provision with: a separate startup-only account when ``CLICKHOUSE_PROVISION_USER`` is set.

    Creating users and views needs access management, which the long-running application user should not hold.
    The provisioner is opened for this block and closed immediately; the application's own client is never
    closed here.
    """
    settings = config.load_clickhouse_provisioner_config()
    if settings is None:
        yield default_admin
        return
    client = ClickHouseClient(settings)
    try:
        yield client
    finally:
        client.close()


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


@dataclasses.dataclass(frozen=True)
class ConsoleAccess:
    """A console identity's client plus the views it must read each fact table through."""

    client: Any
    tables: Mapping[str, str]


class ConsoleClients:
    """Lazy per-(service, mask mode) ClickHouse clients that log in as that identity."""

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
        self._access: dict[tuple[str, bool], ConsoleAccess] = {}
        self._lock = threading.Lock()

    def client_for(self, service_id: str, masked: bool = True) -> ConsoleAccess | None:
        """The identity for ``service_id``, or None when no secret/ClickHouse is configured (no analyst console)."""
        if self._secret is None or self._settings is None:
            return None
        key = (service_id, masked)
        with self._lock:
            existing = self._access.get(key)
        if existing is not None:
            return existing
        # The isolation check is a ClickHouse round trip: never hold the shared lock across it, or one slow
        # service would stall the console for every other service.
        user, password = console_identity(service_id, self._secret, masked)
        client = self._factory(dataclasses.replace(self._settings, user=user, password=password))
        access = ConsoleAccess(client, console_tables(service_id, masked))
        try:
            self._assert_isolated(access, service_id, masked)
        except BaseException:
            client.close()
            raise
        with self._lock:
            winner = self._access.setdefault(key, access)
        if winner is not access:
            client.close()
        return winner

    @staticmethod
    def _assert_isolated(access: ConsoleAccess, service_id: str, masked: bool) -> None:
        """Fail closed unless the identity reads only its own service and, when masked, only redacted identifiers."""
        sid = _literal(service_id)
        for view in access.tables.values():
            rows = access.client.execute(f"SELECT count() AS n FROM {view} WHERE service_id != {sid}")
            if not rows or int(rows[0].get("n", 1)) != 0:
                raise RuntimeError(f"console identity for {service_id} failed the row isolation check on {view}")
        if masked:
            red = _literal(REDACTED)
            unmasked = {
                "request_facts": (
                    f"(client_ip != '' AND client_ip != {red}) "
                    f"OR custom_fields['cookie_session'] NOT IN ('', {red}) "
                    f"OR custom_fields['cid'] NOT IN ('', {red})"
                ),
                "rum_vitals_facts": f"client_id != '' AND client_id != {red}",
                "rum_error_facts": f"client_id != '' AND client_id != {red}",
            }
            for table, condition in unmasked.items():
                view = access.tables[table]
                rows = access.client.execute(f"SELECT count() AS n FROM {view} WHERE {condition}")
                if not rows or int(rows[0].get("n", 1)) != 0:
                    raise RuntimeError(f"console identity for {service_id} failed the masking check on {view}")

    def close(self) -> None:
        with self._lock:
            held, self._access = list(self._access.values()), {}
        for access in held:
            access.client.close()


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
