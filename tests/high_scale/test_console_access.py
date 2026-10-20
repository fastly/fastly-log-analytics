"""Per-service least-privilege ClickHouse identities for analyst SQL.

The console's name-based guards sit on top of this; the database is what makes the boundary hold even if a
future lexical gap lets a query through them.
"""

from __future__ import annotations

import dataclasses
from typing import Any

import pytest

from backend.config import ClickHouseConfig
from backend.high_scale import console_access as access

pytestmark = pytest.mark.security_regression

SECRET = bytes(range(40, 88))


class _Admin:
    def __init__(self, fail_on: str | None = None, policy_tables: tuple[str, ...] | None = None) -> None:
        self.statements: list[str] = []
        self._fail_on = fail_on
        self._policy_tables = access.SCOPED_TABLES if policy_tables is None else policy_tables

    def execute(self, sql: str, params: dict | None = None, *, settings: dict | None = None) -> list[dict]:
        if self._fail_on and self._fail_on in sql:
            raise RuntimeError("denied")
        self.statements.append(sql)
        if "system.row_policies" in sql:
            return [{"table": table} for table in self._policy_tables]
        return []


def test_identity_is_deterministic_per_service_and_unique_across_services() -> None:
    a1 = access.console_identity("svc-a", SECRET)
    a2 = access.console_identity("svc-a", SECRET)
    b = access.console_identity("svc-b", SECRET)

    assert a1 == a2
    assert a1[0] != b[0] and a1[1] != b[1]
    assert a1[0].startswith("fla_console_") and len(a1[1]) >= 48


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


def test_the_row_policy_exists_before_any_select_grant_so_a_failure_leaves_no_access() -> None:
    admin = _Admin()

    access.provision_console_access(admin, "fla_db", "svc-a", SECRET)

    kinds = [
        "policy" if "ROW POLICY" in s else "grant" if s.startswith("GRANT") else "user" if "USER" in s else "other"
        for s in admin.statements
    ]
    assert kinds.index("policy") < kinds.index("grant"), admin.statements
    assert kinds.index("user") < kinds.index("policy")


def test_a_failed_policy_statement_aborts_before_granting_anything() -> None:
    admin = _Admin(fail_on="ROW POLICY")

    with pytest.raises(RuntimeError):
        access.provision_console_access(admin, "fla_db", "svc-a", SECRET)

    assert not any(s.startswith("GRANT") for s in admin.statements)


def test_grants_cover_only_the_scoped_tables_and_nothing_broader() -> None:
    admin = _Admin()

    access.provision_console_access(admin, "fla_db", "svc-a", SECRET)

    grants = [s for s in admin.statements if s.startswith("GRANT")]
    granted = {s.split(" ON ")[1].split(" TO ")[0] for s in grants}
    assert granted == {f"`fla_db`.`{table}`" for table in access.SCOPED_TABLES}
    assert all(s.startswith("GRANT SELECT ON ") for s in grants), "SELECT only, never ALL/INSERT/ALTER"
    assert not any("*" in s for s in grants)


def test_every_scoped_table_gets_a_restrictive_policy_pinned_to_the_service() -> None:
    admin = _Admin()

    access.provision_console_access(admin, "fla_db", "svc-a", SECRET)

    policies = [s for s in admin.statements if "ROW POLICY" in s]
    assert policies and all("AS RESTRICTIVE" in s for s in policies)
    joined = " ".join(policies)
    for table in access.SCOPED_TABLES:
        assert f"`fla_db`.`{table}`" in joined
    assert "service_id = 'svc-a'" in joined


def test_a_hostile_service_id_cannot_break_out_of_the_policy_literal() -> None:
    admin = _Admin()

    access.provision_console_access(admin, "fla_db", "x' OR 1=1 --", SECRET)

    policy = next(s for s in admin.statements if "ROW POLICY" in s)
    assert "x\\' OR 1=1 --" in policy and "x' OR" not in policy


def test_the_profile_pins_readonly_and_caps_every_resource_the_client_sends() -> None:
    admin = _Admin()

    access.provision_console_access(admin, "fla_db", "svc-a", SECRET)

    profile = next(s for s in admin.statements if "SETTINGS PROFILE" in s and s.startswith("CREATE"))
    assert "readonly = 1" in profile
    assert "max_memory_usage = 1073741824 MAX 1073741824" in profile
    assert "max_threads = 2 MAX 2" in profile
    assert "additional_table_filters CHANGEABLE_IN_READONLY" in profile


def test_the_database_name_must_be_a_simple_identifier() -> None:
    with pytest.raises(ValueError):
        access.provision_console_access(_Admin(), "db`; DROP", "svc-a", SECRET)


def test_provisioning_rotates_the_password_for_an_existing_user() -> None:
    admin = _Admin()

    access.provision_console_access(admin, "fla_db", "svc-a", SECRET)

    _, password = access.console_identity("svc-a", SECRET)
    assert any(s.startswith("ALTER USER") and password in s for s in admin.statements)


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


def _manager(monkeypatch: pytest.MonkeyPatch, *, secret: bytes | None = SECRET, leaks: int = 0):
    built: list[_FakeClient] = []

    def factory(settings: ClickHouseConfig) -> _FakeClient:
        client = _FakeClient(settings, leaks)
        built.append(client)
        return client

    manager = access.ConsoleClients(settings=_settings(), secret=secret, client_factory=factory)
    return manager, built


def test_no_secret_means_no_console_client(monkeypatch: pytest.MonkeyPatch) -> None:
    manager, built = _manager(monkeypatch, secret=None)

    assert manager.client_for("svc-a") is None
    assert not built


def test_the_client_logs_in_as_the_services_own_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    manager, built = _manager(monkeypatch)

    client = manager.client_for("svc-a")

    user, password = access.console_identity("svc-a", SECRET)
    assert client is built[0]
    assert (built[0].settings.user, built[0].settings.password) == (user, password)
    assert built[0].settings.database == "fla_db"


def test_each_service_gets_its_own_cached_client(monkeypatch: pytest.MonkeyPatch) -> None:
    manager, built = _manager(monkeypatch)

    first = manager.client_for("svc-a")
    again = manager.client_for("svc-a")
    other = manager.client_for("svc-b")

    assert first is again and first is not other
    assert len(built) == 2


def test_a_client_that_can_see_another_service_is_refused_and_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    manager, built = _manager(monkeypatch, leaks=3)

    with pytest.raises(RuntimeError, match="isolation"):
        manager.client_for("svc-a")

    assert built[0].closed
    assert "service_id != 'svc-a'" in built[0].sql[0]


def test_close_all_closes_every_cached_client(monkeypatch: pytest.MonkeyPatch) -> None:
    manager, built = _manager(monkeypatch)
    manager.client_for("svc-a")
    manager.client_for("svc-b")

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


def test_existing_users_policies_and_profiles_are_repaired_not_left_stale() -> None:
    admin = _Admin()

    access.provision_console_access(admin, "fla_db", "svc-a", SECRET)

    user, _ = access.console_identity("svc-a", SECRET)
    assert any(s.startswith(f"ALTER USER `{user}`") and "SETTINGS PROFILE" in s for s in admin.statements)
    assert any(s.startswith("ALTER ROW POLICY") and "service_id = 'svc-a'" in s for s in admin.statements)


def test_provisioning_verifies_all_scoped_tables_have_the_policy_before_granting() -> None:
    admin = _Admin(policy_tables=("request_facts",))  # one table silently lacks its policy

    with pytest.raises(RuntimeError, match="row polic"):
        access.provision_console_access(admin, "fla_db", "svc-a", SECRET)

    assert not any(s.startswith("GRANT") for s in admin.statements)


def test_the_profile_adds_per_user_memory_and_result_size_ceilings() -> None:
    admin = _Admin()

    access.provision_console_access(admin, "fla_db", "svc-a", SECRET)

    profile = next(s for s in admin.statements if s.startswith("CREATE SETTINGS PROFILE"))
    assert "max_memory_usage_for_user = 2147483648 MAX 2147483648" in profile
    assert "max_result_bytes = 67108864" in profile and "max_result_bytes = 67108864 MAX" not in profile


def test_the_isolation_probe_covers_every_scoped_table(monkeypatch: pytest.MonkeyPatch) -> None:
    manager, built = _manager(monkeypatch)

    manager.client_for("svc-a")

    probed = " ".join(built[0].sql)
    for table in access.SCOPED_TABLES:
        assert f"FROM {table} WHERE service_id != 'svc-a'" in probed


def test_a_leak_in_any_single_scoped_table_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    class _LeakyOne(_FakeClient):
        def execute(self, sql, params=None, *, settings=None):
            self.sql.append(sql)
            return [{"n": 1 if "FROM rum_error_facts" in sql else 0}]

    manager = access.ConsoleClients(settings=_settings(), secret=SECRET, client_factory=_LeakyOne)

    with pytest.raises(RuntimeError, match="isolation"):
        manager.client_for("svc-a")


def test_a_slow_identity_check_for_one_service_does_not_block_another(monkeypatch: pytest.MonkeyPatch) -> None:
    import threading

    release = threading.Event()
    entered = threading.Event()

    class _Slow(_FakeClient):
        def execute(self, sql, params=None, *, settings=None):
            if self.settings.user == access.console_identity("slow", SECRET)[0]:
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
