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
    return next(s for s in _stmts(admin, "CREATE VIEW") if f"`{view}`" in s)


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
    assert "if(isNull(`client_ip`) OR `client_ip` = '', `client_ip`, '[redacted]') AS `client_ip`" in view
    assert "mapApply((k, v) -> (k, if(lower(k) IN (" in view and "AS `custom_fields`" in view and "AS `cmcd`" in view
    for key in ("cookie_session", "cid", "ip", "client_ip", "client_id", "sid", "x-forwarded-for", "true-client-ip"):
        assert f"'{key}'" in view


def test_a_masked_view_lists_its_columns_explicitly_so_a_new_base_column_is_invisible_not_leaked() -> None:
    admin = _Admin()

    access.provision_console_access(admin, "fla_db", "svc-a", SECRET)

    for table, columns in _SCHEMAS.items():
        view = _view_stmt(admin, "svc-a", True, table)
        select = view.split(" AS SELECT ", 1)[1].split(" FROM ", 1)[0]
        assert not re.search(r"(^|,\s)\*(\s|,|$)", select), "no SELECT *: columns added later must not pass through"
        for name, _ in columns:
            assert f"`{name}`" in select


def test_a_masked_view_refuses_to_pass_through_a_map_it_cannot_redact() -> None:
    class _Odd(_Admin):
        def execute(self, sql, params=None, *, settings=None):
            if sql.startswith("DESCRIBE TABLE") and "`request_facts`" in sql:
                return [{"name": "service_id", "type": "String"}, {"name": "tags", "type": "Map(String, UInt64)"}]
            return super().execute(sql, params, settings=settings)

    admin = _Odd()

    with pytest.raises(RuntimeError, match="cannot redact"):
        access.provision_console_access(admin, "fla_db", "svc-a", SECRET)

    assert not _stmts(admin, "GRANT")


@pytest.mark.parametrize("type_", ["IPv4", "IPv6", "Nullable(IPv6)", "UInt32"])
def test_a_masked_view_refuses_a_sensitive_column_whose_type_it_cannot_redact(type_: str) -> None:
    class _Odd(_Admin):
        def execute(self, sql, params=None, *, settings=None):
            if sql.startswith("DESCRIBE TABLE") and "`request_facts`" in sql:
                return [{"name": "service_id", "type": "String"}, {"name": "client_ip", "type": type_}]
            return super().execute(sql, params, settings=settings)

    with pytest.raises(RuntimeError, match="cannot redact"):
        access.provision_console_access(_Odd(), "fla_db", "svc-a", SECRET)


@pytest.mark.parametrize(
    "type_", ["Map(String, String)", "Map(LowCardinality(String), String)", "Map(String, Nullable(String))"]
)
def test_string_keyed_string_maps_of_any_wrapping_are_redacted(type_: str) -> None:
    class _Maps(_Admin):
        def execute(self, sql, params=None, *, settings=None):
            if sql.startswith("DESCRIBE TABLE") and "`request_facts`" in sql:
                return [{"name": "service_id", "type": "String"}, {"name": "m", "type": type_}]
            return super().execute(sql, params, settings=settings)

    admin = _Maps()

    access.provision_console_access(admin, "fla_db", "svc-a", SECRET)

    assert "AS `m`" in _view_stmt(admin, "svc-a", True, "request_facts")


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

    views = _stmts(admin, "CREATE VIEW")
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
            if s.startswith("CREATE VIEW") and any(f"`{v}`" in s for v in views.values())
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


def test_any_pre_existing_grants_are_revoked_before_the_identity_is_granted_its_views() -> None:
    admin = _Admin()

    access.provision_console_access(admin, "fla_db", "svc-a", SECRET)

    for masked in (True, False):
        user, _ = access.console_identity("svc-a", SECRET, masked)
        revoke = next(i for i, s in enumerate(admin.statements) if s == f"REVOKE ALL ON *.* FROM `{user}`")
        first_grant = min(
            i for i, s in enumerate(admin.statements) if s.startswith("GRANT") and s.endswith(f"`{user}`")
        )
        assert revoke < first_grant


def test_missing_views_abort_provisioning_before_any_grant() -> None:
    admin = _Admin(hide_views=True)

    with pytest.raises(RuntimeError, match="views missing"):
        access.provision_console_access(admin, "fla_db", "svc-a", SECRET)

    assert not _stmts(admin, "GRANT")


def test_a_failed_view_statement_aborts_before_granting_anything() -> None:
    admin = _Admin(fail_on="CREATE VIEW")

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

    for view in _stmts(admin, "CREATE VIEW"):
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

    probed = " ".join(masked_built[0].sql)
    assert "client_ip != '' AND client_ip != '[redacted]'" in probed
    assert "custom_fields['cookie_session']" in probed and "custom_fields['cid']" in probed
    assert "FROM fla_cv_" in probed and "client_id != ''" in probed, "the RUM views are checked too"
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


# ── startup provisioning runs as a separate, short-lived account when one is configured ─────────────────


def test_provisioning_uses_the_separate_provisioner_account_and_closes_it(monkeypatch: pytest.MonkeyPatch) -> None:
    built: list[_FakeClient] = []
    provisioner = dataclasses.replace(_settings(), user="fla_provisioner", password="prov-pw")
    monkeypatch.setattr(access.config, "load_clickhouse_provisioner_config", lambda: provisioner)
    monkeypatch.setattr(access, "ClickHouseClient", lambda settings: built.append(_FakeClient(settings)) or built[-1])
    app_user_client = _FakeClient(_settings())

    with access.provisioning_client(app_user_client) as admin:
        assert admin is built[0]
        assert (admin.settings.user, admin.settings.password) == ("fla_provisioner", "prov-pw")
        assert not admin.closed

    assert built[0].closed, "the elevated account must not outlive startup provisioning"
    assert not app_user_client.closed


def test_provisioning_falls_back_to_the_application_client_without_a_provisioner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(access.config, "load_clickhouse_provisioner_config", lambda: None)
    app_user_client = _FakeClient(_settings())

    with access.provisioning_client(app_user_client) as admin:
        assert admin is app_user_client

    assert not app_user_client.closed, "the application's own client is never closed by provisioning"


def test_the_provisioner_is_closed_even_when_provisioning_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    built: list[_FakeClient] = []
    monkeypatch.setattr(
        access.config, "load_clickhouse_provisioner_config", lambda: dataclasses.replace(_settings(), user="p")
    )
    monkeypatch.setattr(access, "ClickHouseClient", lambda settings: built.append(_FakeClient(settings)) or built[-1])

    with pytest.raises(RuntimeError), access.provisioning_client(_FakeClient(_settings())):
        raise RuntimeError("provisioning failed")

    assert built[0].closed


# ── schema drift guard ──────────────────────────────────────────────────────────────────────────────────

_COMMON = {
    "service_id", "event_id", "event_timestamp", "ingest_timestamp", "source_object_key", "source_object_version",
    "line_ordinal", "transform_version", "batch_id", "publication_state", "country",
}  # fmt: skip
# Every column of the three fact tables, each reviewed once for client identifiers. A column that is not listed
# here fails this test, so adding one forces the author to decide how a masked analyst identity sees it
# (add it here when it is not an identifier; otherwise extend MASKED_KEYS / `_redaction`).
_REVIEWED_COLUMNS = {
    "request_facts": _COMMON | {"client_ip", "url", "custom_fields", "cmcd"},
    "rum_vitals_facts": _COMMON
    | {"client_id", "request_event_id", "metric_name", "metric_value", "metric_rating", "pathname", "browser", "os"}
    | {"device", "city", "region", "pop", "tls", "ttfb"},
    "rum_error_facts": _COMMON
    | {"client_id", "request_event_id", "error_message", "error_file", "pathname", "error_line", "error_col"}
    | {"browser", "os", "device", "city", "region", "pop", "tls", "ttfb"},
}


def _ddl_columns() -> dict[str, set[str]]:
    from pathlib import Path

    found: dict[str, set[str]] = {}
    for name in ("request_schema.sql", "rum_schema.sql"):
        text = (Path("backend/high_scale/sql") / name).read_text()
        for match in re.finditer(
            r"CREATE TABLE IF NOT EXISTS (request_facts|rum_vitals_facts|rum_error_facts)\s*\((.*?)\)\s*ENGINE",
            text,
            re.S,
        ):
            columns, depth, current = [], 0, ""
            for char in match.group(2):
                depth += char == "("
                depth -= char == ")"
                if char == "," and depth == 0:
                    columns.append(current.strip())
                    current = ""
                else:
                    current += char
            columns.append(current.strip())
            found[match.group(1)] = {c.split()[0] for c in columns if c}
    return found


def test_every_fact_table_column_has_been_reviewed_for_client_identifiers() -> None:
    declared = _ddl_columns()

    assert set(declared) == set(_REVIEWED_COLUMNS)
    for table, columns in declared.items():
        unreviewed = columns - _REVIEWED_COLUMNS[table]
        assert not unreviewed, (
            f"{table} gained {sorted(unreviewed)}: decide how a masked analyst sees it "
            "(console_access.MASKED_KEYS / _redaction) and then list it in _REVIEWED_COLUMNS"
        )
        assert not (_REVIEWED_COLUMNS[table] - columns), f"{table}: reviewed columns no longer in the DDL"


def test_every_masked_key_that_is_a_column_name_is_handled_by_the_redaction() -> None:
    declared = _ddl_columns()

    for table, columns in declared.items():
        for column in columns & set(access.MASKED_KEYS):
            type_ = "String"
            assert access._redaction(column, type_, "'k'") is not None, f"{table}.{column} would pass through"


def test_views_are_dropped_and_created_never_replaced_so_no_drop_table_privilege_is_needed() -> None:
    admin = _Admin()

    access.provision_console_access(admin, "fla_db", "svc-a", SECRET)

    assert not any("OR REPLACE VIEW" in s or "DROP TABLE" in s for s in admin.statements)
    for masked in (True, False):
        for view in access.console_tables("svc-a", masked).values():
            drop = next(i for i, s in enumerate(admin.statements) if s == f"DROP VIEW IF EXISTS `fla_db`.`{view}`")
            create = next(i for i, s in enumerate(admin.statements) if s.startswith(f"CREATE VIEW `fla_db`.`{view}`"))
            assert drop < create


# ── Merged from test_query_console_scope.py ─────────────────────────────────

from types import SimpleNamespace

from backend.high_scale import query as console
from backend.high_scale.query import _reject_unsafe_functions, _table_filters, query_endpoint
from backend.models.dashboard import QueryRequest

START = "2026-09-15T00:00:00+00:00"
END = "2026-09-15T01:00:00+00:00"


class _Recorder:
    def __init__(self, probe_rows: int = 0) -> None:
        self.calls: list[tuple[str, dict[str, str]]] = []
        self._probe_rows = probe_rows

    def execute(self, sql: str, params: dict | None = None, *, settings: dict[str, str] | None = None) -> list[dict]:
        self.calls.append((sql, dict(settings or {})))
        if "system.one" in sql:
            return [{"n": self._probe_rows}]
        return []


def _run(sql: str, *, max_rows: int = 10, client: _Recorder | None = None) -> tuple[str, dict[str, str]]:
    client = client or _Recorder()
    query_endpoint(
        SimpleNamespace(service_id="svc", client=client),  # type: ignore[arg-type]
        QueryRequest(sql=sql, max_rows=max_rows),
        START,
        END,
        False,
    )
    return client.calls[-1]


def test_every_referenced_fact_table_is_pinned_by_a_setting_not_by_sql_text() -> None:
    sql, settings = _run("SELECT count(*) AS n FROM logs WHERE url = '/x' OR 1 = 1")

    assert "additional_table_filters" not in sql and "SETTINGS" not in sql
    filters = settings["additional_table_filters"]
    assert filters.startswith("{'request_facts': ")
    assert "service_id = \\'svc\\'" in filters
    assert "high_scale_batch_publications FINAL" in filters and "domain = \\'request\\'" in filters
    assert "publication_state = \\'visible\\'" in filters
    assert "toDateTime64(\\'2026-09-15 00:00:00.000000\\', 6, \\'UTC\\')" in filters
    assert "toDateTime64(\\'2026-09-15 01:00:00.000000\\', 6, \\'UTC\\')" in filters


def test_the_users_own_where_clause_is_left_untouched_not_spliced_with_the_scope() -> None:
    sql, _ = _run("SELECT count(*) AS n FROM logs WHERE url = '/x' OR 1 = 1")

    assert "WHERE url = '/x' OR 1 = 1" in sql
    assert "service_id" not in sql


def test_the_row_cap_sits_on_its_own_line_after_the_user_sql() -> None:
    sql, settings = _run("SELECT * FROM logs", max_rows=7)

    assert sql.endswith("\n) LIMIT 8")
    assert settings["max_result_rows"] == "8" and settings["result_overflow_mode"] == "break"
    assert "additional_table_filters" in settings


def test_an_explicit_user_limit_is_bounded_by_the_global_ceiling_not_trusted() -> None:
    sql, settings = _run("SELECT url FROM logs LIMIT 999999999", max_rows=5)

    assert sql.endswith(f"\n) LIMIT {console.MAX_QUERY_ROWS + 1}")
    assert settings["max_result_rows"] == str(console.MAX_QUERY_ROWS + 1)


def test_each_rum_table_gets_its_own_domain_fence() -> None:
    filters = _table_filters("svc", START, END, ["rum_vitals_facts", "rum_error_facts"])

    assert "'rum_vitals_facts': " in filters and "domain = \\'rum_vitals\\'" in filters
    assert "'rum_error_facts': " in filters and "domain = \\'rum_errors\\'" in filters


def test_a_hostile_service_id_cannot_break_out_of_the_filter_literal() -> None:
    filters = _table_filters("x' OR 1=1 --", None, None, ["request_facts"])

    # Escaped once for the inner filter literal and again for the outer map-value literal.
    assert "x" + "\\" * 3 + "' OR 1=1 --" in filters
    assert "x' OR" not in filters


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM remote('other-host', default.request_facts)",
        "SELECT * FROM url('http://169.254.169.254/latest/meta-data', 'CSV', 'x String')",
        "SELECT * FROM file('/etc/passwd', 'CSV', 'x String')",
        "SELECT * FROM numbers(1000000000)",
        "SELECT getSetting('max_memory_usage') FROM logs",
        "SELECT hostName(), version(), currentUser() FROM logs",
        "SELECT * FROM \"remote\"('h', db.t)",
        "SELECT hex(ip) FROM logs",
        "SELECT left(ip, 3) FROM logs",
        "SELECT right(ip, 3) FROM logs",
        "SELECT dictGet('d', 'a', 1) FROM logs",
    ],
)
def test_clickhouse_table_and_server_functions_are_rejected(sql: str) -> None:
    with pytest.raises(PermissionError):
        _reject_unsafe_functions(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT count(*) AS n, quantile(0.95)(elapsed) AS p95 FROM logs GROUP BY toStartOfMinute(timestamp)",
        "SELECT url FROM logs WHERE country IN ('US', 'CA') AND (status = '200' OR status = '404')",
        "SELECT row_number() OVER (PARTITION BY country ORDER BY n DESC) AS rk FROM (SELECT 1 AS n) AS t",
        "SELECT 'remote(' AS s, '-- not a comment' AS c FROM logs",
        "SELECT count(*) FROM logs -- remote('x')",
        "SELECT count(*) FROM logs /* url('x') */",
    ],
)
def test_ordinary_analytics_sql_is_not_rejected(sql: str) -> None:
    _reject_unsafe_functions(sql)


def test_the_console_refuses_to_serve_when_the_server_does_not_enforce_table_filters() -> None:
    ignoring = _Recorder(probe_rows=1)

    with pytest.raises(RuntimeError, match="not enforcing"):
        _run("SELECT count(*) FROM logs", client=ignoring)

    assert all("FROM (" not in sql for sql, _ in ignoring.calls), "no user query may run after a failed probe"


def test_a_passing_probe_is_remembered_per_client() -> None:
    client: Any = _Recorder()

    _run("SELECT 1 AS a FROM logs", client=client)
    _run("SELECT 2 AS a FROM logs", client=client)

    assert sum("system.one" in sql for sql, _ in client.calls) == 1


@pytest.mark.parametrize("decoy", ["'LIMIT'", "'it''s a LIMIT'"])
def test_the_word_limit_in_a_literal_does_not_raise_the_users_row_cap(decoy: str) -> None:
    sql, settings = _run(f"SELECT {decoy} AS a FROM logs", max_rows=5)

    assert sql.endswith("\n) LIMIT 6")
    assert settings["max_result_rows"] == "6"


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM logs -- swallow the rest",
        "SELECT * FROM logs /* c */",
        "SELECT 1 # hash comment",
        "SELECT * FROM remote/**/('h', 'd', 't')",
        "SELECT hostName--\n() FROM logs",
        "SELECT 1 /* /* */ FROM system.tables\n-- */ */",
        "SELECT * FROM logs WHERE url = 'a\\'' OR 1 = 1 --'",
        "SELECT $$x$$ FROM logs",
        "SELECT `ip` FROM logs",
        "SELECT * FROM logs SETTINGS additional_table_filters = '{}'",
        "SELECT * FROM logs FORMAT TSV",
        "SELECT * FROM logs INTO OUTFILE '/tmp/x'",
        "SELECT * FROM logs WHERE url = 'unterminated",
        "SELECT * FROM default.logs",
        "SELECT * FROM fastly_log_analytics.request_facts",
        "SELECT * FROM logs WHERE country = 'US\u200b'  AND x\u00a0= 1",
        # A quote inside a double-quoted identifier must not open a fake string literal that hides code.
        'SELECT 1 AS "a\'" /* /* */ FROM system.tables\n-- */ , 2 AS "b\'" FROM logs',
        "SELECT 1 AS \"a'\" /* /* */ FROM remote('h', x.y)\n-- */ , 2 AS \"b'\" FROM logs",
        'SELECT "url" FROM logs',
        # A recursive CTE named after a real table puts that name in scope for its own anchor branch.
        "WITH RECURSIVE high_scale_batch_publications AS (SELECT * FROM high_scale_batch_publications "
        "UNION ALL SELECT * FROM high_scale_batch_publications WHERE 1 = 0) SELECT * FROM high_scale_batch_publications",
        "with recursive t as (select 1 as n union all select n + 1 from t where n < 3) select n from t",
        'SELECT 1 AS "a\'", 2 AS "b\'" FROM logs',
    ],
)
def test_syntax_the_two_parsers_could_read_differently_is_rejected_before_validation(sql: str) -> None:
    client = _Recorder()

    with pytest.raises(PermissionError):
        _run(sql, client=client)

    assert not client.calls, "nothing may reach ClickHouse"


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT city FROM logs WHERE city = 'São Paulo'",
        "SELECT format('{}', 1) AS f FROM logs",
        "SELECT url AS settings_url FROM logs",
        "SELECT 'a -- b /* c */ # d' AS s FROM logs",
        "SELECT 'it''s' AS s FROM logs",
        "SELECT logs.url FROM logs",
        "SELECT 'a\"b' AS s, 'c\"d' AS t FROM logs",
    ],
)
def test_ordinary_sql_still_runs(sql: str) -> None:
    _run(sql)


# ── analyst identities read through views ──────────────────────────────────────────────────────────────


VIEWS = {
    "request_facts": "fla_cv_abc_m_request_facts",
    "rum_vitals_facts": "fla_cv_abc_m_rum_vitals_facts",
    "rum_error_facts": "fla_cv_abc_m_rum_error_facts",
}


def _run_as_analyst(sql: str, client: _Recorder | None = None) -> tuple[str, dict[str, str]]:
    client = client or _Recorder()
    query_endpoint(
        SimpleNamespace(service_id="svc", client=None),  # type: ignore[arg-type]
        QueryRequest(sql=sql, max_rows=10),
        START,
        END,
        True,
        client=client,
        tables=VIEWS,
    )
    return client.calls[-1]


def test_analyst_sql_is_renamed_onto_the_identitys_views_never_the_base_tables() -> None:
    sql, _ = _run_as_analyst("SELECT count(*) AS n FROM logs")

    assert "fla_cv_abc_m_request_facts" in sql
    assert not re.search(r"(?<![\w])request_facts", sql.replace("fla_cv_abc_m_request_facts", ""))


@pytest.mark.parametrize(
    ("user_table", "view"),
    [("client_vitals", "rum_vitals_facts"), ("client_errors", "rum_error_facts")],
)
def test_rum_tables_map_to_their_own_views(user_table: str, view: str) -> None:
    sql, settings = _run_as_analyst(f"SELECT count(*) AS n FROM {user_table}")

    assert VIEWS[view] in sql
    assert f"'{VIEWS[view]}'" in settings["additional_table_filters"]


def test_with_a_view_only_the_analyst_window_is_sent_as_a_setting() -> None:
    _, settings = _run_as_analyst("SELECT count(*) AS n FROM logs")

    scope = settings["additional_table_filters"]
    assert scope.startswith("{'fla_cv_abc_m_request_facts': ")
    assert "event_timestamp >=" in scope and "event_timestamp <=" in scope
    for fixed_by_the_view in ("service_id", "publication_state", "high_scale_batch_publications"):
        assert fixed_by_the_view not in scope, "the identity cannot read the publication table a fence subquery needs"


def test_table_filters_with_a_view_and_no_window_sends_no_setting_at_all() -> None:
    assert _table_filters("svc", None, None, {"v": "request"}, scoped_by_view=True) is None


def test_table_filters_default_still_pins_service_fence_and_window_for_the_privileged_client() -> None:
    filters = _table_filters("svc", START, END, {"request_facts": "request"})

    assert filters is not None
    assert "service_id = \\'svc\\'" in filters and "high_scale_batch_publications" in filters


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM fla_cv_abc_m_request_facts",
        "SELECT * FROM fla_cv_abc_r_request_facts",
        "SELECT * FROM default.fla_cv_abc_m_request_facts",
        "SELECT * FROM request_facts",
        "SELECT * FROM high_scale_batch_publications",
    ],
)
def test_an_analyst_cannot_name_a_view_or_base_table_directly(sql: str) -> None:
    client = _Recorder()

    with pytest.raises(PermissionError):
        _run_as_analyst(sql, client)

    assert not client.calls


def test_the_privileged_path_without_views_is_unchanged() -> None:
    sql, settings = _run("SELECT count(*) AS n FROM logs")

    assert "request_facts" in sql and "fla_cv_" not in sql
    assert "service_id" in settings["additional_table_filters"]
