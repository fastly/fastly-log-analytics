"""Per-service, per-mask-mode least-privilege ClickHouse identities for analyst SQL (view-based).

The console's name-based guards sit on top of this; the database is what makes the boundary hold even if a
future lexical gap lets a query through them.
"""

from __future__ import annotations

import dataclasses
import re
import threading
from typing import Any

import pytest

from backend.config import ClickHouseConfig
from backend.high_scale import console_access as access

pytestmark = pytest.mark.security_regression

SECRET = bytes(range(40, 88))

_SCHEMAS = {
    "request_facts": [
        ("service_id", "LowCardinality(String)"),
        ("event_timestamp", "DateTime64(3, 'UTC')"),
        ("batch_id", "UUID"),
        ("publication_state", "Enum8('pending' = 1, 'visible' = 2)"),
        ("country", "LowCardinality(String)"),
        ("client_ip", "String"),
        ("url", "String"),
        ("custom_fields", "Map(String, String)"),
        ("cmcd", "Map(String, String)"),
    ],
    "rum_vitals_facts": [
        ("service_id", "LowCardinality(String)"),
        ("event_timestamp", "DateTime64(3, 'UTC')"),
        ("client_id", "String"),
        ("request_event_id", "Nullable(UUID)"),
        ("metric_name", "LowCardinality(String)"),
    ],
    "rum_error_facts": [
        ("service_id", "LowCardinality(String)"),
        ("event_timestamp", "DateTime64(3, 'UTC')"),
        ("client_id", "Nullable(String)"),
        ("error_message", "String"),
    ],
}


class _Admin:
    def __init__(self, fail_on: str | None = None, hide_views: bool = False) -> None:
        self.statements: list[str] = []
        self._fail_on = fail_on
        self._hide_views = hide_views

    def execute(self, sql: str, params: dict | None = None, *, settings: dict | None = None) -> list[dict]:
        if self._fail_on and self._fail_on in sql:
            raise RuntimeError("denied")
        self.statements.append(sql)
        if sql.startswith("DESCRIBE TABLE"):
            table = re.search(r"`([a-z_]+)`$", sql).group(1)  # type: ignore[union-attr]
            return [{"name": name, "type": type_} for name, type_ in _SCHEMAS[table]]
        if "FROM system.tables" in sql:
            return [] if self._hide_views else [{"name": n} for n in re.findall(r"'(fla_cv_[a-z0-9_]+)'", sql)]
        return []


def _stmts(admin: _Admin, prefix: str) -> list[str]:
    return [s for s in admin.statements if s.startswith(prefix)]


def _view_stmt(admin: _Admin, service_id: str, masked: bool, table: str) -> str:
    view = access.console_tables(service_id, masked)[table]
    return next(s for s in _stmts(admin, "CREATE OR REPLACE VIEW") if f"`{view}`" in s)


# ── identities ──────────────────────────────────────────────────────────────────────────────────────────


def test_identity_is_deterministic_per_service_and_unique_across_services_and_modes() -> None:
    a1 = access.console_identity("svc-a", SECRET)
    assert a1 == access.console_identity("svc-a", SECRET)
    others = {
        access.console_identity("svc-b", SECRET),
        access.console_identity("svc-a", SECRET, masked=False),
    }
    assert a1 not in others and len(others) == 2
    assert all(user.startswith("fla_console_") for user, _ in {a1, *others})
    assert all(len(password) >= 48 for _, password in {a1, *others})
    assert a1[0].endswith("_m") and access.console_identity("svc-a", SECRET, masked=False)[0].endswith("_r")


def test_identity_depends_on_the_secret() -> None:
    assert access.console_identity("svc", SECRET) != access.console_identity("svc", bytes(range(90, 138)))


@pytest.mark.parametrize("secret", [b"", b"short", b"x" * 31])
def test_a_weak_console_secret_is_rejected(secret: bytes) -> None:
    with pytest.raises(ValueError, match="at least 32"):
        access.console_identity("svc", secret)


@pytest.mark.parametrize("secret", [b"a" * 48, b"ab" * 30, b"0123" * 12])
def test_a_long_but_low_diversity_secret_is_rejected(secret: bytes) -> None:
    with pytest.raises(ValueError, match="distinct"):
        access.console_identity("svc", secret)


def test_each_mode_reads_through_its_own_views() -> None:
    masked = access.console_tables("svc-a", masked=True)
    raw = access.console_tables("svc-a", masked=False)

    assert set(masked) == set(access.FACT_DOMAINS) == set(raw)
    assert not set(masked.values()) & set(raw.values())
    assert not set(masked.values()) & set(access.console_tables("svc-b", masked=True).values())


# ── provisioning DDL ────────────────────────────────────────────────────────────────────────────────────


def test_a_masked_view_replaces_client_ips_and_sensitive_map_keys_in_the_data() -> None:
    admin = _Admin()

    access.provision_console_access(admin, "fla_db", "svc-a", SECRET)

    view = _view_stmt(admin, "svc-a", True, "request_facts")
    assert "SELECT * REPLACE (" in view
    assert "if(isNull(`client_ip`) OR `client_ip` = '', `client_ip`, '[redacted]') AS `client_ip`" in view
    assert "mapApply((k, v) -> (k, if(lower(k) IN (" in view and "AS `custom_fields`" in view and "AS `cmcd`" in view
    for key in ("cookie_session", "cid", "ip", "client_ip", "client_id"):
        assert f"'{key}'" in view


def test_the_rum_client_id_is_redacted_in_masked_rum_views_including_nullable_columns() -> None:
    admin = _Admin()

    access.provision_console_access(admin, "fla_db", "svc-a", SECRET)

    assert "AS `client_id`" in _view_stmt(admin, "svc-a", True, "rum_vitals_facts")
    assert "AS `client_id`" in _view_stmt(admin, "svc-a", True, "rum_error_facts")


def test_a_raw_view_is_a_plain_projection_without_redaction() -> None:
    admin = _Admin()

    access.provision_console_access(admin, "fla_db", "svc-a", SECRET)

    view = _view_stmt(admin, "svc-a", False, "request_facts")
    assert "SELECT * FROM `fla_db`.`request_facts`" in view
    assert "SELECT * REPLACE (" not in view and "[redacted]" not in view


def test_every_view_fixes_the_service_and_the_publication_fence_and_runs_with_definer_rights() -> None:
    admin = _Admin()

    access.provision_console_access(admin, "fla_db", "svc-a", SECRET)

    views = _stmts(admin, "CREATE OR REPLACE VIEW")
    assert len(views) == 6  # 3 tables x 2 modes
    for view in views:
        assert "DEFINER = CURRENT_USER SQL SECURITY DEFINER" in view
        assert "WHERE service_id = 'svc-a' AND publication_state = 'visible'" in view
        assert "FROM `fla_db`.`high_scale_batch_publications` FINAL WHERE service_id = 'svc-a'" in view
        assert "publication_state = 'visible')" in view


def test_the_publication_fence_names_the_right_domain_per_table() -> None:
    admin = _Admin()

    access.provision_console_access(admin, "fla_db", "svc-a", SECRET)

    for table, domain in access.FACT_DOMAINS.items():
        assert f"domain = '{domain}'" in _view_stmt(admin, "svc-a", True, table)


def test_grants_are_select_on_the_identitys_own_views_only() -> None:
    admin = _Admin()

    access.provision_console_access(admin, "fla_db", "svc-a", SECRET)

    grants = _stmts(admin, "GRANT")
    assert all(s.startswith("GRANT SELECT ON ") for s in grants)
    for masked in (True, False):
        user, _ = access.console_identity("svc-a", SECRET, masked)
        own = [s for s in grants if s.endswith(f"TO `{user}`")]
        granted = {s.split(" ON ")[1].split(" TO ")[0] for s in own}
        assert granted == {f"`fla_db`.`{view}`" for view in access.console_tables("svc-a", masked).values()}
    on_objects = {s.split(" ON ")[1].split(" TO ")[0] for s in grants}
    assert all(obj.startswith("`fla_db`.`fla_cv_") for obj in on_objects), "views only: no base table, no publications"
    assert not any("*" in s for s in grants)


def test_each_users_views_are_created_and_verified_before_that_user_is_granted_anything() -> None:
    admin = _Admin()

    access.provision_console_access(admin, "fla_db", "svc-a", SECRET)

    for masked in (True, False):
        user, _ = access.console_identity("svc-a", SECRET, masked)
        views = access.console_tables("svc-a", masked)
        last_view = max(
            i
            for i, s in enumerate(admin.statements)
            if s.startswith("CREATE OR REPLACE VIEW") and any(f"`{v}`" in s for v in views.values())
        )
        verify = next(
            i
            for i, s in enumerate(admin.statements)
            if "FROM system.tables" in s and f"'{next(iter(views.values()))}'" in s
        )
        first_grant = min(
            i for i, s in enumerate(admin.statements) if s.startswith("GRANT") and s.endswith(f"`{user}`")
        )
        assert last_view < verify < first_grant


def test_missing_views_abort_provisioning_before_any_grant() -> None:
    admin = _Admin(hide_views=True)

    with pytest.raises(RuntimeError, match="views missing"):
        access.provision_console_access(admin, "fla_db", "svc-a", SECRET)

    assert not _stmts(admin, "GRANT")


def test_a_failed_view_statement_aborts_before_granting_anything() -> None:
    admin = _Admin(fail_on="CREATE OR REPLACE VIEW")

    with pytest.raises(RuntimeError):
        access.provision_console_access(admin, "fla_db", "svc-a", SECRET)

    assert not _stmts(admin, "GRANT")


def test_pre_view_identities_with_base_table_grants_are_removed() -> None:
    admin = _Admin()

    access.provision_console_access(admin, "fla_db", "svc-a", SECRET)

    legacy = access.console_identity("svc-a", SECRET)[0].removesuffix("_m")
    assert any(s.startswith("DROP ROW POLICY IF EXISTS") and legacy in s for s in admin.statements)
    assert f"DROP USER IF EXISTS `{legacy}`" in admin.statements


def test_provisioning_refuses_when_a_base_table_has_no_columns() -> None:
    class _Empty(_Admin):
        def execute(self, sql, params=None, *, settings=None):
            return [] if sql.startswith("DESCRIBE") else super().execute(sql, params, settings=settings)

    with pytest.raises(RuntimeError, match="no columns"):
        access.provision_console_access(_Empty(), "fla_db", "svc-a", SECRET)


def test_a_hostile_service_id_cannot_break_out_of_any_literal() -> None:
    admin = _Admin()

    access.provision_console_access(admin, "fla_db", "x' OR 1=1 --", SECRET)

    for view in _stmts(admin, "CREATE OR REPLACE VIEW"):
        assert "x\\' OR 1=1 --" in view and "x' OR" not in view


def test_the_profile_pins_readonly_and_caps_every_resource_the_client_sends() -> None:
    admin = _Admin()

    access.provision_console_access(admin, "fla_db", "svc-a", SECRET)

    profile = next(s for s in admin.statements if s.startswith("CREATE SETTINGS PROFILE"))
    assert "readonly = 1" in profile
    assert "max_memory_usage = 1073741824 MAX 1073741824" in profile
    assert "max_threads = 2 MAX 2" in profile
    assert "additional_table_filters CHANGEABLE_IN_READONLY" in profile
    assert "max_memory_usage_for_user = 2147483648 MAX 2147483648" in profile
    assert "max_result_bytes = 67108864" in profile and "max_result_bytes = 67108864 MAX" not in profile


def test_the_database_name_must_be_a_simple_identifier() -> None:
    with pytest.raises(ValueError):
        access.provision_console_access(_Admin(), "db`; DROP", "svc-a", SECRET)


def test_provisioning_rotates_passwords_and_reattaches_the_profile_for_existing_users() -> None:
    admin = _Admin()

    access.provision_console_access(admin, "fla_db", "svc-a", SECRET)

    for masked in (True, False):
        user, password = access.console_identity("svc-a", SECRET, masked)
        assert any(
            s.startswith(f"ALTER USER `{user}`") and password in s and "SETTINGS PROFILE" in s for s in admin.statements
        )


def test_provisioning_is_idempotent_in_shape() -> None:
    first, second = _Admin(), _Admin()

    access.provision_console_access(first, "fla_db", "svc-a", SECRET)
    access.provision_console_access(second, "fla_db", "svc-a", SECRET)

    assert first.statements == second.statements


# ── the runtime client side ────────────────────────────────────────────────────────────────────────────


def _settings() -> ClickHouseConfig:
    return ClickHouseConfig(host="ch", port=8123, database="fla_db", user="app", password="pw", secure=False)


class _FakeClient:
    def __init__(self, settings: ClickHouseConfig, leaks: int = 0) -> None:
        self.settings = settings
        self.leaks = leaks
        self.sql: list[str] = []
        self.closed = False

    def execute(self, sql: str, params: dict | None = None, *, settings: dict | None = None) -> list[dict[str, Any]]:
        self.sql.append(sql)
        return [{"n": self.leaks}]

    def close(self) -> None:
        self.closed = True


def _manager(*, secret: bytes | None = SECRET, leaks: int = 0):
    built: list[_FakeClient] = []

    def factory(settings: ClickHouseConfig) -> _FakeClient:
        client = _FakeClient(settings, leaks)
        built.append(client)
        return client

    return access.ConsoleClients(settings=_settings(), secret=secret, client_factory=factory), built


def test_no_secret_means_no_console_client() -> None:
    manager, built = _manager(secret=None)

    assert manager.client_for("svc-a") is None
    assert not built


def test_the_client_logs_in_as_the_services_own_identity_for_the_requested_mode() -> None:
    manager, built = _manager()

    masked_access = manager.client_for("svc-a", masked=True)
    raw_access = manager.client_for("svc-a", masked=False)

    assert masked_access is not None and raw_access is not None
    for granted, masked in ((masked_access, True), (raw_access, False)):
        user, password = access.console_identity("svc-a", SECRET, masked)
        assert (granted.client.settings.user, granted.client.settings.password) == (user, password)
        assert granted.client.settings.database == "fla_db"
        assert dict(granted.tables) == access.console_tables("svc-a", masked)
    assert masked_access.client is not raw_access.client and len(built) == 2


def test_each_service_and_mode_gets_its_own_cached_client() -> None:
    manager, built = _manager()

    first = manager.client_for("svc-a")
    again = manager.client_for("svc-a")
    other = manager.client_for("svc-b")

    assert first is again and first is not other
    assert len(built) == 2


def test_the_isolation_probe_covers_every_view() -> None:
    manager, built = _manager()

    manager.client_for("svc-a", masked=False)

    probed = " ".join(built[0].sql)
    for view in access.console_tables("svc-a", False).values():
        assert f"FROM {view} WHERE service_id != 'svc-a'" in probed


def test_a_masked_identity_is_also_probed_for_unredacted_client_ips() -> None:
    masked, masked_built = _manager()
    raw, raw_built = _manager()

    masked.client_for("svc-a", masked=True)
    raw.client_for("svc-a", masked=False)

    assert any("client_ip != '' AND client_ip != '[redacted]'" in sql for sql in masked_built[0].sql)
    assert not any("[redacted]" in sql for sql in raw_built[0].sql)


def test_a_client_that_can_see_another_service_is_refused_and_closed() -> None:
    manager, built = _manager(leaks=3)

    with pytest.raises(RuntimeError, match="isolation"):
        manager.client_for("svc-a")

    assert built[0].closed


def test_a_leak_in_any_single_view_is_refused() -> None:
    class _LeakyOne(_FakeClient):
        def execute(self, sql, params=None, *, settings=None):
            self.sql.append(sql)
            return [{"n": 1 if "rum_error_facts WHERE" in sql else 0}]

    manager = access.ConsoleClients(settings=_settings(), secret=SECRET, client_factory=_LeakyOne)

    with pytest.raises(RuntimeError, match="isolation"):
        manager.client_for("svc-a")


def test_a_masked_view_that_does_not_mask_is_refused() -> None:
    class _Unmasked(_FakeClient):
        def execute(self, sql, params=None, *, settings=None):
            self.sql.append(sql)
            return [{"n": 5 if "client_ip != ''" in sql else 0}]

    manager = access.ConsoleClients(settings=_settings(), secret=SECRET, client_factory=_Unmasked)

    with pytest.raises(RuntimeError, match="masking"):
        manager.client_for("svc-a", masked=True)
    assert manager.client_for("svc-a", masked=False) is not None, "the raw identity is not subject to the masking probe"


def test_a_slow_identity_check_for_one_service_does_not_block_another() -> None:
    release, entered = threading.Event(), threading.Event()
    slow_user = access.console_identity("slow", SECRET)[0]

    class _Slow(_FakeClient):
        def execute(self, sql, params=None, *, settings=None):
            if self.settings.user == slow_user:
                entered.set()
                release.wait(5)
            return [{"n": 0}]

    manager = access.ConsoleClients(settings=_settings(), secret=SECRET, client_factory=_Slow)
    worker = threading.Thread(target=lambda: manager.client_for("slow"))
    worker.start()
    assert entered.wait(5)

    assert manager.client_for("fast") is not None  # must not wait for the slow service's ClickHouse call

    release.set()
    worker.join(5)


def test_close_closes_every_cached_client() -> None:
    manager, built = _manager()
    manager.client_for("svc-a")
    manager.client_for("svc-b", masked=False)

    manager.close()

    assert all(client.closed for client in built)


def test_settings_replacement_keeps_every_other_connection_field() -> None:
    base = _settings()

    scoped = dataclasses.replace(base, user="u", password="p")

    assert (scoped.host, scoped.port, scoped.database, scoped.secure) == (
        base.host,
        base.port,
        base.database,
        base.secure,
    )
