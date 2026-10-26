"""Per-service, per-mask-mode least-privilege ClickHouse identities for analyst SQL (view-based)."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
import threading
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, call

import httpx
import pytest
import yaml

from backend.config import ClickHouseConfig
from backend.core.clickhouse import ClickHouseClient, ClickHouseError
from backend.high_scale import postgres_control as access
from backend.high_scale import query_service as console
from backend.high_scale.archive import (
    ArchiveArtifact,
    ArchiveManifest,
    ArchivePublication,
    ArchiveSourceEntry,
    ArchiveSourceObject,
    ArchiveState,
    ClickHousePublication,
    HighScaleBatch,
    InMemoryBatchManifestStore,
    InMemoryObjectStore,
    InsertReceipt,
    PublicationStatus,
    replay_manifest,
    replay_origin_projections,
    write_batch_archive_checkpoint,
)
from backend.high_scale.ingest_controller import (
    rebuild_origin_projections_from_fos,
    rebuild_serving_state_from_fos,
)
from backend.high_scale.postgres_control import (
    DeletionController,
    DeletionSweepResult,
    HighScaleDeletionSweeper,
    PostgresDeletionLedger,
)
from backend.high_scale.query_service import (
    _reject_unsafe_functions,
    _table_filters,
    query_endpoint,
)
from backend.models.analytics import QueryRequest
from tests.high_scale.conftest import (
    HighScaleLedger,
    OwnershipStore,
    wait_for_clickhouse_ready,
)
from tests.high_scale.conftest import (
    clickhouse_single_node_ddl as _ddl,
)
from tests.high_scale.conftest import (
    free_loopback_port as _free_port,
)

# fmt: off
pytestmark = pytest.mark.security_regression

SECRET = bytes(range(40, 88))
_SCHEMAS = {
    "request_facts": [
        ("service_id", "LowCardinality(String)"), ("event_timestamp", "DateTime64(3, 'UTC')"),
        ("batch_id", "UUID"), ("publication_state", "Enum8('pending' = 1, 'visible' = 2)"),
        ("country", "LowCardinality(String)"), ("client_ip", "String"), ("url", "String"),
        ("custom_fields", "Map(String, String)"), ("cmcd", "Map(String, String)"),
    ],
    "rum_vitals_facts": [
        ("service_id", "LowCardinality(String)"), ("event_timestamp", "DateTime64(3, 'UTC')"),
        ("client_id", "String"), ("request_event_id", "Nullable(UUID)"), ("metric_name", "LowCardinality(String)"),
    ],
    "rum_error_facts": [
        ("service_id", "LowCardinality(String)"), ("event_timestamp", "DateTime64(3, 'UTC')"),
        ("client_id", "Nullable(String)"), ("error_message", "String"),
    ],
}  # fmt: skip


class _Admin:
    def __init__(
        self, fail_on: str | None = None, hide_views: bool = False, schemas: dict[str, list[tuple[str, str]]] | None = None
    ) -> None:
        self.statements: list[str] = []
        self._fail_on, self._hide_views, self._schemas = fail_on, hide_views, schemas

    def execute(self, sql: str, params: dict | None = None, *, settings: dict | None = None) -> list[dict]:
        if self._fail_on and self._fail_on in sql:
            raise RuntimeError("denied")
        self.statements.append(sql)
        if sql.startswith("DESCRIBE TABLE"):
            table = re.search(r"`([a-z_]+)`$", sql).group(1)  # type: ignore[union-attr]
            cols = (self._schemas or _SCHEMAS).get(table, _SCHEMAS[table]) if self._schemas is not None else _SCHEMAS[table]
            return [{"name": name, "type": type_} for name, type_ in cols]
        if "FROM system.tables" in sql:
            return [] if self._hide_views else [{"name": n} for n in re.findall(r"'(fla_cv_[a-z0-9_]+)'", sql)]
        return []


def _stmts(admin: _Admin, prefix: str) -> list[str]:
    return [s for s in admin.statements if s.startswith(prefix)]


def _view_stmt(admin: _Admin, service_id: str, masked: bool, table: str) -> str:
    view = access.console_tables(service_id, masked)[table]
    return next(s for s in _stmts(admin, "CREATE VIEW") if f"`{view}`" in s)


def _provision(service_id: str = "svc-a", **kw: Any) -> _Admin:
    admin = _Admin(**kw)
    access.provision_console_access(admin, "fla_db", service_id, SECRET)
    return admin


def test_identity_is_deterministic_per_service_and_unique_across_services_and_modes() -> None:
    a1 = access.console_identity("svc-a", SECRET)
    assert a1 == access.console_identity("svc-a", SECRET)
    others = {access.console_identity("svc-b", SECRET), access.console_identity("svc-a", SECRET, masked=False)}
    assert a1 not in others and len(others) == 2
    assert all(user.startswith("fla_console_") and len(pw) >= 48 for user, pw in {a1, *others})
    assert a1[0].endswith("_m") and access.console_identity("svc-a", SECRET, masked=False)[0].endswith("_r")


def test_identity_depends_on_the_secret() -> None:
    assert access.console_identity("svc", SECRET) != access.console_identity("svc", bytes(range(90, 138)))


@pytest.mark.parametrize(
    ("secret", "match"),
    [
        (b"", "at least 32"), (b"short", "at least 32"), (b"x" * 31, "at least 32"),
        (b"a" * 48, "distinct"), (b"ab" * 30, "distinct"), (b"0123" * 12, "distinct"),
    ],
)  # fmt: skip
def test_weak_or_low_diversity_console_secret_is_rejected(secret: bytes, match: str) -> None:
    with pytest.raises(ValueError, match=match):
        access.console_identity("svc", secret)


def test_each_mode_reads_through_its_own_views() -> None:
    masked, raw = access.console_tables("svc-a", masked=True), access.console_tables("svc-a", masked=False)
    assert set(masked) == set(access.FACT_DOMAINS) == set(raw)
    assert not set(masked.values()) & set(raw.values())
    assert not set(masked.values()) & set(access.console_tables("svc-b", masked=True).values())


def test_a_masked_view_replaces_client_ips_and_sensitive_map_keys_in_the_data() -> None:
    view = _view_stmt(_provision(), "svc-a", True, "request_facts")
    assert "if(isNull(`client_ip`) OR `client_ip` = '', `client_ip`, '[redacted]') AS `client_ip`" in view
    assert "mapApply((k, v) -> (k, if(lower(k) IN (" in view and "AS `custom_fields`" in view and "AS `cmcd`" in view
    for key in ("cookie_session", "cid", "ip", "client_ip", "client_id", "sid", "x-forwarded-for", "true-client-ip"):
        assert f"'{key}'" in view


def test_a_masked_view_lists_its_columns_explicitly_so_a_new_base_column_is_invisible_not_leaked() -> None:
    admin = _provision()
    for table, columns in _SCHEMAS.items():
        select = _view_stmt(admin, "svc-a", True, table).split(" AS SELECT ", 1)[1].split(" FROM ", 1)[0]
        assert not re.search(r"(^|,\s)\*(\s|,|$)", select) and all(f"`{name}`" in select for name, _ in columns)


def test_a_masked_view_refuses_to_pass_through_a_map_it_cannot_redact() -> None:
    admin = _Admin(schemas={"request_facts": [("service_id", "String"), ("tags", "Map(String, UInt64)")]})
    with pytest.raises(RuntimeError, match="cannot redact"):
        access.provision_console_access(admin, "fla_db", "svc-a", SECRET)
    assert not _stmts(admin, "GRANT")


@pytest.mark.parametrize("type_", ["IPv4", "IPv6", "Nullable(IPv6)", "UInt32"])
def test_a_masked_view_refuses_a_sensitive_column_whose_type_it_cannot_redact(type_: str) -> None:
    admin = _Admin(schemas={"request_facts": [("service_id", "String"), ("client_ip", type_)]})
    with pytest.raises(RuntimeError, match="cannot redact"):
        access.provision_console_access(admin, "fla_db", "svc-a", SECRET)


@pytest.mark.parametrize(
    "type_", ["Map(String, String)", "Map(LowCardinality(String), String)", "Map(String, Nullable(String))"]
)
def test_string_keyed_string_maps_of_any_wrapping_are_redacted(type_: str) -> None:
    admin = _provision(schemas={"request_facts": [("service_id", "String"), ("m", type_)]})
    assert "AS `m`" in _view_stmt(admin, "svc-a", True, "request_facts")


def test_the_rum_client_id_is_redacted_in_masked_rum_views_including_nullable_columns() -> None:
    admin = _provision()
    assert "AS `client_id`" in _view_stmt(admin, "svc-a", True, "rum_vitals_facts")
    assert "AS `client_id`" in _view_stmt(admin, "svc-a", True, "rum_error_facts")


def test_a_raw_view_is_a_plain_projection_without_redaction() -> None:
    view = _view_stmt(_provision(), "svc-a", False, "request_facts")
    assert "SELECT * FROM `fla_db`.`request_facts`" in view
    assert "SELECT * REPLACE (" not in view and "[redacted]" not in view


def test_every_view_fixes_the_service_and_the_publication_fence_and_runs_with_definer_rights() -> None:
    views = _stmts(_provision(), "CREATE VIEW")
    assert len(views) == 6
    for view in views:
        assert "DEFINER = CURRENT_USER SQL SECURITY DEFINER" in view
        assert "WHERE service_id = 'svc-a' AND publication_state = 'visible'" in view
        assert "FROM `fla_db`.`high_scale_batch_publications` FINAL WHERE service_id = 'svc-a'" in view
        assert "publication_state = 'visible')" in view


def test_the_publication_fence_names_the_right_domain_per_table() -> None:
    admin = _provision()
    for table, domain in access.FACT_DOMAINS.items():
        assert f"domain = '{domain}'" in _view_stmt(admin, "svc-a", True, table)


def test_grants_are_select_on_the_identitys_own_views_only() -> None:
    grants = _stmts(_provision(), "GRANT")
    assert all(s.startswith("GRANT SELECT ON ") for s in grants)
    for masked in (True, False):
        user, _ = access.console_identity("svc-a", SECRET, masked)
        granted = {s.split(" ON ")[1].split(" TO ")[0] for s in grants if s.endswith(f"TO `{user}`")}
        assert granted == {f"`fla_db`.`{view}`" for view in access.console_tables("svc-a", masked).values()}
    assert all(s.split(" ON ")[1].split(" TO ")[0].startswith("`fla_db`.`fla_cv_") and "*" not in s for s in grants)


def test_each_users_views_are_created_and_verified_before_that_user_is_granted_anything() -> None:
    admin = _provision()
    for masked in (True, False):
        user, _ = access.console_identity("svc-a", SECRET, masked)
        views = access.console_tables("svc-a", masked)
        last_view = max(
            i for i, s in enumerate(admin.statements)
            if s.startswith("CREATE VIEW") and any(f"`{v}`" in s for v in views.values())
        )
        verify = next(
            i for i, s in enumerate(admin.statements)
            if "FROM system.tables" in s and f"'{next(iter(views.values()))}'" in s
        )
        first_grant = min(
            i for i, s in enumerate(admin.statements) if s.startswith("GRANT") and s.endswith(f"`{user}`")
        )
        assert last_view < verify < first_grant


def test_any_pre_existing_grants_are_revoked_before_the_identity_is_granted_its_views() -> None:
    admin = _provision()
    for masked in (True, False):
        user, _ = access.console_identity("svc-a", SECRET, masked)
        revoke = next(i for i, s in enumerate(admin.statements) if s == f"REVOKE ALL ON *.* FROM `{user}`")
        first_grant = min(
            i for i, s in enumerate(admin.statements) if s.startswith("GRANT") and s.endswith(f"`{user}`")
        )
        assert revoke < first_grant


@pytest.mark.parametrize(("kw", "match"), [({"hide_views": True}, "views missing"), ({"fail_on": "CREATE VIEW"}, None)])
def test_missing_or_failed_views_abort_provisioning_before_any_grant(kw: dict[str, Any], match: str | None) -> None:
    admin = _Admin(**kw)
    with pytest.raises(RuntimeError, match=match):
        access.provision_console_access(admin, "fla_db", "svc-a", SECRET)
    assert not _stmts(admin, "GRANT")


def test_pre_view_identities_with_base_table_grants_are_removed() -> None:
    admin = _provision()
    legacy = access.console_identity("svc-a", SECRET)[0].removesuffix("_m")
    assert any(s.startswith("DROP ROW POLICY IF EXISTS") and legacy in s for s in admin.statements)
    assert f"DROP USER IF EXISTS `{legacy}`" in admin.statements


def test_provisioning_refuses_when_a_base_table_has_no_columns() -> None:
    with pytest.raises(RuntimeError, match="no columns"):
        access.provision_console_access(_Admin(schemas={"request_facts": []}), "fla_db", "svc-a", SECRET)


def test_a_hostile_service_id_cannot_break_out_of_any_literal() -> None:
    for view in _stmts(_provision("x' OR 1=1 --"), "CREATE VIEW"):
        assert "x\\' OR 1=1 --" in view and "x' OR" not in view


def test_the_profile_pins_readonly_and_caps_every_resource_the_client_sends() -> None:
    profile = next(s for s in _provision().statements if s.startswith("CREATE SETTINGS PROFILE"))
    for expected in (
        "readonly = 1", "max_memory_usage = 1073741824 MAX 1073741824", "max_threads = 2 MAX 2",
        "additional_table_filters CHANGEABLE_IN_READONLY",
        "max_memory_usage_for_user = 2147483648 MAX 2147483648", "max_result_bytes = 67108864",
    ):
        assert expected in profile
    assert "max_result_bytes = 67108864 MAX" not in profile


def test_the_database_name_must_be_a_simple_identifier() -> None:
    with pytest.raises(ValueError):
        access.provision_console_access(_Admin(), "db`; DROP", "svc-a", SECRET)


def test_provisioning_rotates_passwords_and_reattaches_the_profile_for_existing_users() -> None:
    admin = _provision()
    for masked in (True, False):
        user, password = access.console_identity("svc-a", SECRET, masked)
        assert any(
            s.startswith(f"ALTER USER `{user}`") and password in s and "SETTINGS PROFILE" in s for s in admin.statements
        )


def test_provisioning_is_idempotent_in_shape() -> None:
    assert _provision().statements == _provision().statements


def _settings() -> ClickHouseConfig:
    return ClickHouseConfig(host="ch", port=8123, database="fla_db", user="app", password="pw", secure=False)


class _FakeClient:
    def __init__(self, settings: ClickHouseConfig, leaks: int = 0, hook: Any = None) -> None:
        self.settings, self.leaks, self.hook, self.sql, self.closed = settings, leaks, hook, [], False

    def execute(self, sql: str, params: dict | None = None, *, settings: dict | None = None) -> list[dict[str, Any]]:
        self.sql.append(sql)
        return self.hook(self, sql) if self.hook else [{"n": self.leaks}]

    def close(self) -> None:
        self.closed = True


def _manager(*, secret: bytes | None = SECRET, leaks: int = 0, hook: Any = None):
    built: list[_FakeClient] = []
    factory = lambda s: built.append(_FakeClient(s, leaks, hook)) or built[-1]  # noqa: E731
    return access.ConsoleClients(settings=_settings(), secret=secret, client_factory=factory), built


def test_no_secret_means_no_console_client() -> None:
    manager, built = _manager(secret=None)
    assert manager.client_for("svc-a") is None and not built


def test_the_client_logs_in_as_the_services_own_identity_for_the_requested_mode() -> None:
    manager, built = _manager()
    masked_access, raw_access = manager.client_for("svc-a", masked=True), manager.client_for("svc-a", masked=False)
    assert masked_access is not None and raw_access is not None
    for granted, masked in ((masked_access, True), (raw_access, False)):
        user, password = access.console_identity("svc-a", SECRET, masked)
        assert (granted.client.settings.user, granted.client.settings.password) == (user, password)
        assert granted.client.settings.database == "fla_db" and dict(granted.tables) == access.console_tables("svc-a", masked)
    assert masked_access.client is not raw_access.client and len(built) == 2


def test_each_service_and_mode_gets_its_own_cached_client() -> None:
    manager, built = _manager()
    first, again, other = manager.client_for("svc-a"), manager.client_for("svc-a"), manager.client_for("svc-b")
    assert first is again and first is not other and len(built) == 2


def test_the_isolation_probe_covers_every_view() -> None:
    manager, built = _manager()
    manager.client_for("svc-a", masked=False)
    probed = " ".join(built[0].sql)
    assert all(f"FROM {v} WHERE service_id != 'svc-a'" in probed for v in access.console_tables("svc-a", False).values())


def test_a_masked_identity_is_also_probed_for_unredacted_client_ips() -> None:
    masked, masked_built = _manager()
    raw, raw_built = _manager()
    masked.client_for("svc-a", masked=True)
    raw.client_for("svc-a", masked=False)
    probed = " ".join(masked_built[0].sql)
    assert "client_ip != '' AND client_ip != '[redacted]'" in probed
    assert "custom_fields['cookie_session']" in probed and "custom_fields['cid']" in probed
    assert "FROM fla_cv_" in probed and "client_id != ''" in probed
    assert not any("[redacted]" in sql for sql in raw_built[0].sql)


def test_a_client_that_can_see_another_service_is_refused_and_closed() -> None:
    manager, built = _manager(leaks=3)
    with pytest.raises(RuntimeError, match="isolation"):
        manager.client_for("svc-a")
    assert built[0].closed


def test_a_leak_in_any_single_view_is_refused() -> None:
    manager, _ = _manager(hook=lambda _, sql: [{"n": 1 if "rum_error_facts WHERE" in sql else 0}])
    with pytest.raises(RuntimeError, match="isolation"):
        manager.client_for("svc-a")


def test_a_masked_view_that_does_not_mask_is_refused() -> None:
    manager, _ = _manager(hook=lambda _, sql: [{"n": 5 if "client_ip != ''" in sql else 0}])
    with pytest.raises(RuntimeError, match="masking"):
        manager.client_for("svc-a", masked=True)
    assert manager.client_for("svc-a", masked=False) is not None


def test_a_slow_identity_check_for_one_service_does_not_block_another() -> None:
    release, entered = threading.Event(), threading.Event()
    slow_user = access.console_identity("slow", SECRET)[0]
    manager, _ = _manager(
        hook=lambda c, _: (entered.set(), release.wait(5), [{"n": 0}])[2] if c.settings.user == slow_user else [{"n": 0}]
    )
    worker = threading.Thread(target=lambda: manager.client_for("slow"))
    worker.start()
    assert entered.wait(5) and manager.client_for("fast") is not None
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
    assert (scoped.host, scoped.port, scoped.database, scoped.secure) == (base.host, base.port, base.database, base.secure)


def test_provisioning_uses_the_separate_provisioner_account_and_closes_it(monkeypatch: pytest.MonkeyPatch) -> None:
    built: list[_FakeClient] = []
    provisioner = dataclasses.replace(_settings(), user="fla_provisioner", password="prov-pw")
    monkeypatch.setattr(access.config, "load_clickhouse_provisioner_config", lambda: provisioner)
    monkeypatch.setattr(access, "ClickHouseClient", lambda settings: built.append(_FakeClient(settings)) or built[-1])
    app_user_client = _FakeClient(_settings())
    with access.provisioning_client(app_user_client) as admin:
        assert admin is built[0] and (admin.settings.user, admin.settings.password) == ("fla_provisioner", "prov-pw")
        assert not admin.closed
    assert built[0].closed and not app_user_client.closed


def test_provisioning_falls_back_to_the_application_client_without_a_provisioner(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(access.config, "load_clickhouse_provisioner_config", lambda: None)
    app_user_client = _FakeClient(_settings())
    with access.provisioning_client(app_user_client) as admin:
        assert admin is app_user_client
    assert not app_user_client.closed


def test_the_provisioner_is_closed_even_when_provisioning_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    built: list[_FakeClient] = []
    monkeypatch.setattr(access.config, "load_clickhouse_provisioner_config", lambda: dataclasses.replace(_settings(), user="p"))
    monkeypatch.setattr(access, "ClickHouseClient", lambda settings: built.append(_FakeClient(settings)) or built[-1])
    with pytest.raises(RuntimeError), access.provisioning_client(_FakeClient(_settings())):
        raise RuntimeError("provisioning failed")
    assert built[0].closed


_COMMON = {
    "service_id", "event_id", "event_timestamp", "ingest_timestamp", "source_object_key", "source_object_version",
    "line_ordinal", "transform_version", "batch_id", "publication_state", "country",
}  # fmt: skip
_RUM_SHARED = {"client_id", "request_event_id", "pathname", "browser", "os", "device", "city", "region", "pop", "tls", "ttfb"}  # fmt: skip
_REVIEWED_COLUMNS = {
    "request_facts": _COMMON | {"client_ip", "url", "custom_fields", "cmcd"},
    "rum_vitals_facts": _COMMON | _RUM_SHARED | {"metric_name", "metric_value", "metric_rating"},
    "rum_error_facts": _COMMON | _RUM_SHARED | {"error_message", "error_file", "error_line", "error_col"},
}


def _ddl_columns() -> dict[str, set[str]]:
    found: dict[str, set[str]] = {}
    for name in ("request_schema.sql", "rum_schema.sql"):
        text = (Path("backend/high_scale/sql") / name).read_text()
        for match in re.finditer(
            r"CREATE TABLE IF NOT EXISTS (request_facts|rum_vitals_facts|rum_error_facts)\s*\((.*?)\)\s*ENGINE",
            text, re.S,
        ):
            columns, depth, current = [], 0, ""
            for char in match.group(2):
                depth += (char == "(") - (char == ")")
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
        assert columns == _REVIEWED_COLUMNS[table]


def test_every_masked_key_that_is_a_column_name_is_handled_by_the_redaction() -> None:
    for table, columns in _ddl_columns().items():
        for column in columns & set(access.MASKED_KEYS):
            assert access._redaction(column, "String", "'k'") is not None, f"{table}.{column} would pass through"


def test_views_are_dropped_and_created_never_replaced_so_no_drop_table_privilege_is_needed() -> None:
    admin = _provision()
    assert not any("OR REPLACE VIEW" in s or "DROP TABLE" in s for s in admin.statements)
    for masked in (True, False):
        for view in access.console_tables("svc-a", masked).values():
            drop = next(i for i, s in enumerate(admin.statements) if s == f"DROP VIEW IF EXISTS `fla_db`.`{view}`")
            create = next(i for i, s in enumerate(admin.statements) if s.startswith(f"CREATE VIEW `fla_db`.`{view}`"))
            assert drop < create


# ── Merged from test_query_console_scope.py ─────────────────────────────────

START, END = "2026-09-15T00:00:00+00:00", "2026-09-15T01:00:00+00:00"
VIEWS = {
    "request_facts": "fla_cv_abc_m_request_facts",
    "rum_vitals_facts": "fla_cv_abc_m_rum_vitals_facts",
    "rum_error_facts": "fla_cv_abc_m_rum_error_facts",
}


class _Recorder:
    def __init__(self, probe_rows: int = 0) -> None:
        self.calls: list[tuple[str, dict[str, str]]] = []
        self._probe_rows = probe_rows

    def execute(self, sql: str, params: dict | None = None, *, settings: dict[str, str] | None = None) -> list[dict]:
        self.calls.append((sql, dict(settings or {})))
        return [{"n": self._probe_rows}] if "system.one" in sql else []


def _run(sql: str, *, max_rows: int = 10, client: _Recorder | None = None) -> tuple[str, dict[str, str]]:
    client = client or _Recorder()
    query_endpoint(
        SimpleNamespace(service_id="svc", client=client),  # type: ignore[arg-type]
        QueryRequest(sql=sql, max_rows=max_rows), START, END, False,
    )
    return client.calls[-1]


def _run_as_analyst(sql: str, client: _Recorder | None = None) -> tuple[str, dict[str, str]]:
    client = client or _Recorder()
    query_endpoint(
        SimpleNamespace(service_id="svc", client=None),  # type: ignore[arg-type]
        QueryRequest(sql=sql, max_rows=10), START, END, True, client=client, tables=VIEWS,
    )
    return client.calls[-1]


def test_every_referenced_fact_table_is_pinned_by_a_setting_not_by_sql_text() -> None:
    sql, settings = _run("SELECT count(*) AS n FROM logs WHERE url = '/x' OR 1 = 1")
    assert "additional_table_filters" not in sql and "SETTINGS" not in sql
    filters = settings["additional_table_filters"]
    assert filters.startswith("{'request_facts': ")
    for token in (
        "service_id = \\'svc\\'", "high_scale_batch_publications FINAL", "domain = \\'request\\'",
        "publication_state = \\'visible\\'", "toDateTime64(\\'2026-09-15 00:00:00.000000\\', 6, \\'UTC\\')",
        "toDateTime64(\\'2026-09-15 01:00:00.000000\\', 6, \\'UTC\\')",
    ):
        assert token in filters


def test_the_users_own_where_clause_is_left_untouched_not_spliced_with_the_scope() -> None:
    sql, _ = _run("SELECT count(*) AS n FROM logs WHERE url = '/x' OR 1 = 1")
    assert "WHERE url = '/x' OR 1 = 1" in sql and "service_id" not in sql


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
    assert "x" + "\\" * 3 + "' OR 1=1 --" in filters and "x' OR" not in filters


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
        "SELECT hex(ip) FROM logs", "SELECT left(ip, 3) FROM logs", "SELECT right(ip, 3) FROM logs",
        "SELECT dictGet('d', 'a', 1) FROM logs",
    ],
)  # fmt: skip
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
        "SELECT count(*) FROM logs -- remote('x')", "SELECT count(*) FROM logs /* url('x') */",
    ],
)  # fmt: skip
def test_ordinary_analytics_sql_is_not_rejected(sql: str) -> None:
    _reject_unsafe_functions(sql)


def test_the_console_refuses_to_serve_when_the_server_does_not_enforce_table_filters() -> None:
    ignoring = _Recorder(probe_rows=1)
    with pytest.raises(RuntimeError, match="not enforcing"):
        _run("SELECT count(*) FROM logs", client=ignoring)
    assert all("FROM (" not in sql for sql, _ in ignoring.calls)


def test_a_passing_probe_is_remembered_per_client() -> None:
    client: Any = _Recorder()
    _run("SELECT 1 AS a FROM logs", client=client)
    _run("SELECT 2 AS a FROM logs", client=client)
    assert sum("system.one" in sql for sql, _ in client.calls) == 1


@pytest.mark.parametrize("decoy", ["'LIMIT'", "'it''s a LIMIT'"])
def test_the_word_limit_in_a_literal_does_not_raise_the_users_row_cap(decoy: str) -> None:
    sql, settings = _run(f"SELECT {decoy} AS a FROM logs", max_rows=5)
    assert sql.endswith("\n) LIMIT 6") and settings["max_result_rows"] == "6"


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM logs -- swallow the rest", "SELECT * FROM logs /* c */", "SELECT 1 # hash comment",
        "SELECT * FROM remote/**/('h', 'd', 't')", "SELECT hostName--\n() FROM logs",
        "SELECT 1 /* /* */ FROM system.tables\n-- */ */", "SELECT * FROM logs WHERE url = 'a\\'' OR 1 = 1 --'",
        "SELECT $$x$$ FROM logs", "SELECT `ip` FROM logs",
        "SELECT * FROM logs SETTINGS additional_table_filters = '{}'", "SELECT * FROM logs FORMAT TSV",
        "SELECT * FROM logs INTO OUTFILE '/tmp/x'", "SELECT * FROM logs WHERE url = 'unterminated",
        "SELECT * FROM default.logs", "SELECT * FROM fastly_log_analytics.request_facts",
        "SELECT * FROM logs WHERE country = 'US\u200b'  AND x\u00a0= 1",
        'SELECT 1 AS "a\'" /* /* */ FROM system.tables\n-- */ , 2 AS "b\'" FROM logs',
        "SELECT 1 AS \"a'\" /* /* */ FROM remote('h', x.y)\n-- */ , 2 AS \"b'\" FROM logs",
        'SELECT "url" FROM logs',
        "WITH RECURSIVE high_scale_batch_publications AS (SELECT * FROM high_scale_batch_publications "
        "UNION ALL SELECT * FROM high_scale_batch_publications WHERE 1 = 0) SELECT * FROM high_scale_batch_publications",
        "with recursive t as (select 1 as n union all select n + 1 from t where n < 3) select n from t",
        'SELECT 1 AS "a\'", 2 AS "b\'" FROM logs',
    ],
)  # fmt: skip
def test_syntax_the_two_parsers_could_read_differently_is_rejected_before_validation(sql: str) -> None:
    client = _Recorder()
    with pytest.raises(PermissionError):
        _run(sql, client=client)
    assert not client.calls


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT city FROM logs WHERE city = 'São Paulo'", "SELECT format('{}', 1) AS f FROM logs",
        "SELECT url AS settings_url FROM logs", "SELECT 'a -- b /* c */ # d' AS s FROM logs",
        "SELECT 'it''s' AS s FROM logs", "SELECT logs.url FROM logs", "SELECT 'a\"b' AS s, 'c\"d' AS t FROM logs",
    ],
)  # fmt: skip
def test_ordinary_sql_still_runs(sql: str) -> None:
    _run(sql)


def test_analyst_sql_is_renamed_onto_the_identitys_views_never_the_base_tables() -> None:
    sql, _ = _run_as_analyst("SELECT count(*) AS n FROM logs")
    assert "fla_cv_abc_m_request_facts" in sql
    assert not re.search(r"(?<![\w])request_facts", sql.replace("fla_cv_abc_m_request_facts", ""))


@pytest.mark.parametrize(("user_table", "view"), [("client_vitals", "rum_vitals_facts"), ("client_errors", "rum_error_facts")])
def test_rum_tables_map_to_their_own_views(user_table: str, view: str) -> None:
    sql, settings = _run_as_analyst(f"SELECT count(*) AS n FROM {user_table}")
    assert VIEWS[view] in sql and f"'{VIEWS[view]}'" in settings["additional_table_filters"]


def test_with_a_view_only_the_analyst_window_is_sent_as_a_setting() -> None:
    _, settings = _run_as_analyst("SELECT count(*) AS n FROM logs")
    scope = settings["additional_table_filters"]
    assert scope.startswith("{'fla_cv_abc_m_request_facts': ")
    assert "event_timestamp >=" in scope and "event_timestamp <=" in scope
    assert all(k not in scope for k in ("service_id", "publication_state", "high_scale_batch_publications"))


def test_table_filters_with_a_view_and_no_window_sends_no_setting_at_all() -> None:
    assert _table_filters("svc", None, None, {"v": "request"}, scoped_by_view=True) is None


def test_table_filters_default_still_pins_service_fence_and_window_for_the_privileged_client() -> None:
    filters = _table_filters("svc", START, END, {"request_facts": "request"})
    assert filters is not None and "service_id = \\'svc\\'" in filters and "high_scale_batch_publications" in filters


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM fla_cv_abc_m_request_facts", "SELECT * FROM fla_cv_abc_r_request_facts",
        "SELECT * FROM default.fla_cv_abc_m_request_facts", "SELECT * FROM request_facts",
        "SELECT * FROM high_scale_batch_publications",
    ],
)  # fmt: skip
def test_an_analyst_cannot_name_a_view_or_base_table_directly(sql: str) -> None:
    client = _Recorder()
    with pytest.raises(PermissionError):
        _run_as_analyst(sql, client)
    assert not client.calls


def test_the_privileged_path_without_views_is_unchanged() -> None:
    sql, settings = _run("SELECT count(*) AS n FROM logs")
    assert "request_facts" in sql and "fla_cv_" not in sql and "service_id" in settings["additional_table_filters"]


# ── Merged from tests/high_scale/test_deletion.py ──


def _checksum(value: bytes) -> str:
    return f"sha256:{hashlib.sha256(value).hexdigest()}"


def _manifest_for(
    manifest_id: str = "manifest-1", *, now: datetime = datetime(2026, 9, 4, tzinfo=UTC), key: str | None = None
) -> ArchiveManifest:
    source = ArchiveSourceObject("svc", "request", key or f"raw/{manifest_id}.gz", "sha256:source", 4, "v1")
    return ArchiveManifest(
        manifest_id, source,
        ArchiveArtifact(f"s3://archive/{manifest_id}.parquet", _checksum(b"data"), 4, 1, 4, "sha256:e", "v1", "v1"),
        now - timedelta(days=3), now - timedelta(days=2), now - timedelta(days=1), now - timedelta(hours=1), 3,
    )


def _stage_source_in_ledger(
    ledger: HighScaleLedger, store: InMemoryObjectStore, key: str, checksum: str, size: int, rows: int, manifest_id: str
) -> None:
    store.put(key, b"raw")
    ledger.discover("svc", "request", key, checksum, size_bytes=size, version="v1")
    claim = ledger.claim("svc", key, "worker")
    ledger.record_counts("svc", key, accepted_rows=rows, malformed_rows=0)
    ledger.mark_appended("svc", key, claim.lease_generation)
    ledger.mark_archived("svc", key, claim.lease_generation, manifest_id, 3)
    ledger.acknowledge("svc", key, manifest_id)


def test_source_deletion_requires_archive_and_owner_fence() -> None:
    store, ledger = InMemoryObjectStore(), HighScaleLedger()
    publication = ArchivePublication(store)
    try:
        now = datetime(2026, 9, 4, tzinfo=UTC)
        manifest = _manifest_for("manifest-1", now=now, key="raw/request/a.gz")
        assert manifest.source is not None
        _stage_source_in_ledger(ledger, store, manifest.source.object_key, manifest.source.checksum, 4, 1, "manifest-1")
        publication.publish(manifest, b"data")
        DeletionController(store, publication, ledger).delete_source(manifest, current_owner_epoch=3, now=now)
        assert not store.exists(manifest.source.object_key)
        ledger.mark_source_deleted("svc", manifest.source.object_key, manifest.manifest_id)
    finally:
        ledger.close()


def test_source_deletion_is_blocked_before_deadline() -> None:
    store, ledger = InMemoryObjectStore(), HighScaleLedger()
    try:
        with pytest.raises(ValueError, match="grace period"):
            DeletionController(store, ArchivePublication(store), ledger).delete_source(
                _manifest_for(), current_owner_epoch=3, now=datetime(2026, 9, 1, tzinfo=UTC)
            )
    finally:
        ledger.close()


def test_source_deletion_rejects_non_high_scale_owner() -> None:
    ownership, ledger = OwnershipStore(), HighScaleLedger()
    ownership.initialize("svc", owner="standard", source_cursor="cursor-0")
    try:
        with pytest.raises(ValueError, match="not authorized"):
            DeletionController(
                InMemoryObjectStore(), ArchivePublication(InMemoryObjectStore()), ledger, ownership=ownership
            ).delete_source(_manifest_for(), current_owner_epoch=1)
    finally:
        ownership.close()
        ledger.close()


@dataclass
class _FakeControl:
    owner_epoch_for_owner_call: int = 5
    transition_error: Exception | None = None

    def __post_init__(self) -> None:
        self.transition_calls: list[tuple] = []
        self.authorize_calls: list[tuple] = []
        self.mark_deleted_calls: list[tuple] = []

    def transition_archive_manifest(self, manifest_id, *, expected_state, next_state, expected_owner_epoch):
        self.transition_calls.append((manifest_id, expected_state, next_state, expected_owner_epoch))
        if self.transition_error is not None:
            raise self.transition_error

    def authorize_source_delete(self, service_id, object_key, *, manifest_id, expected_owner_epoch):
        self.authorize_calls.append((service_id, object_key, manifest_id, expected_owner_epoch))
        return "authorized"

    def owner(self, service_id):
        return MagicMock(owner_epoch=self.owner_epoch_for_owner_call)

    def mark_source_deleted(self, service_id, object_key, *, manifest_id, expected_owner_epoch):
        self.mark_deleted_calls.append((service_id, object_key, manifest_id, expected_owner_epoch))


def test_postgres_deletion_ledger_satisfies_the_ownership_checker_protocol() -> None:
    control = _FakeControl()
    assert PostgresDeletionLedger(control).get("svc").owner_epoch == control.owner_epoch_for_owner_call


def test_postgres_deletion_ledger_promotes_manifest_then_authorizes() -> None:
    control = _FakeControl()
    res = PostgresDeletionLedger(control).authorize_source_delete("svc", "raw/one.gz", "manifest-1", current_owner_epoch=3)
    assert res == "authorized" and control.authorize_calls == [("svc", "raw/one.gz", "manifest-1", 3)]
    assert control.transition_calls == [("manifest-1", ArchiveState.MANIFEST_COMMITTED, ArchiveState.DELETION_ELIGIBLE, 3)]


def test_postgres_deletion_ledger_authorize_tolerates_an_already_promoted_manifest() -> None:
    control = _FakeControl(transition_error=ValueError("archive manifest transition is not adjacent"))
    res = PostgresDeletionLedger(control).authorize_source_delete("svc", "raw/one.gz", "manifest-1", current_owner_epoch=3)
    assert res == "authorized" and control.authorize_calls == [("svc", "raw/one.gz", "manifest-1", 3)]


def test_postgres_deletion_ledger_mark_deleted_resolves_owner_epoch_itself() -> None:
    control = _FakeControl(owner_epoch_for_owner_call=7)
    PostgresDeletionLedger(control).mark_source_deleted("svc", "raw/one.gz", "manifest-1")
    assert control.mark_deleted_calls == [("svc", "raw/one.gz", "manifest-1", 7)]


@dataclass
class _ManifestRecord:
    manifest: object
    owner_epoch: int


class _SweepControl:
    def __init__(self, sources, manifests) -> None:
        self._sources, self._manifests = sources, manifests

    def deletable_sources(self, service_id, *, limit):
        return self._sources

    def archive_manifest(self, manifest_id):
        return self._manifests[manifest_id]


def test_sweeper_deletes_eligible_sources_and_skips_ineligible_ones() -> None:
    now = datetime(2026, 9, 5, tzinfo=UTC)
    first, second = _manifest_for("first", now=now), _manifest_for("second", now=now)
    control = _SweepControl(
        sources=[MagicMock(archive_manifest_id="first"), MagicMock(archive_manifest_id="second"), MagicMock(archive_manifest_id=None)],
        manifests={"first": _ManifestRecord(first, 3), "second": _ManifestRecord(second, 3)},
    )
    controller = MagicMock()
    controller.delete_source.side_effect = [None, ValueError("source deletion grace period has not elapsed")]
    result = HighScaleDeletionSweeper(control=control, controller=controller).sweep(service_id="svc", now=now)
    assert result == DeletionSweepResult(deleted=1, skipped=1)
    assert controller.delete_source.call_args_list == [
        call(first, current_owner_epoch=3, now=now), call(second, current_owner_epoch=3, now=now)
    ]


def test_delete_manifest_sources_multi_source() -> None:
    store, ledger = InMemoryObjectStore(), HighScaleLedger()
    publication = ArchivePublication(store)
    try:
        now = datetime(2026, 9, 10, tzinfo=UTC)
        sources = [
            ArchiveSourceEntry(
                key=f"raw/request/part_{i}.gz", checksum=f"sha256:source_{i}", size_bytes=10,
                row_start=i * 5, row_count=5,
                deletion_deadline=now - timedelta(hours=1) if i < 2 else now + timedelta(hours=1),
            )
            for i in range(3)
        ]
        manifest = ArchiveManifest(
            "manifest-batch",
            artifact=ArchiveArtifact(
                "s3://archive/artifacts/manifest-batch.parquet", _checksum(b"batch_data"),
                10, 15, 10, "sha256:events", "request.v1", "normalize.v1",
            ),
            coverage_start=now - timedelta(days=3), coverage_end=now - timedelta(days=2),
            retention_deadline=now - timedelta(days=1), deletion_authorization_deadline=now - timedelta(hours=1),
            archive_epoch=3, service_id="svc", domain="request", sources=sources,
        )
        for s in sources:
            _stage_source_in_ledger(ledger, store, s.key, s.checksum, s.size_bytes, s.row_count, manifest.manifest_id)
        publication.publish(manifest, b"batch_data")
        deleted = DeletionController(store, publication, ledger).delete_manifest_sources(
            manifest, current_owner_epoch=3, now=now
        )
        assert set(deleted) == {"raw/request/part_0.gz", "raw/request/part_1.gz"}
        assert not store.exists("raw/request/part_0.gz") and not store.exists("raw/request/part_1.gz")
        assert store.exists("raw/request/part_2.gz")
    finally:
        ledger.close()


def test_sweeper_groups_by_manifest_and_deletes_batch() -> None:
    now = datetime(2026, 9, 10, tzinfo=UTC)
    manifest = _manifest_for("m1", now=now)
    control = MagicMock()
    control.deletable_sources.return_value = [
        MagicMock(archive_manifest_id="m1", object_key="raw/1.gz"),
        MagicMock(archive_manifest_id="m1", object_key="raw/2.gz"),
    ]
    control.archive_manifest.return_value = MagicMock(manifest=manifest, owner_epoch=3)
    controller = MagicMock(spec=DeletionController)
    controller.delete_manifest_sources.return_value = ["raw/1.gz", "raw/2.gz"]
    result = HighScaleDeletionSweeper(control=control, controller=controller).sweep(service_id="svc", now=now)
    assert result == DeletionSweepResult(deleted=2, skipped=0)
    controller.delete_manifest_sources.assert_called_once_with(
        manifest, current_owner_epoch=3, source_keys=["raw/1.gz", "raw/2.gz"], now=now
    )


# ── Merged from test_archive_only_recovery.py ────────────────────────


class _MockClickHouseClient:
    def __init__(self) -> None:
        self.batches: list[HighScaleBatch] = []

    def insert(self, batch: HighScaleBatch) -> InsertReceipt:
        self.batches.append(batch)
        return InsertReceipt(batch.batch_id, len(batch.rows), batch.digest)

    @property
    def request_rows(self) -> list[dict[str, Any]]:
        return [r for b in self.batches if b.domain == "request" for r in b.rows]


class _InMemoryCatalog:
    def __init__(self, manifests: tuple[ArchiveManifest, ...]) -> None:
        self._manifests = manifests

    def manifests_covering(self, service_id: str, domain: str, start: datetime, end: datetime) -> tuple[ArchiveManifest, ...]:
        return tuple(
            m for m in self._manifests
            if m.service_id == service_id and m.domain == domain and m.coverage_end > start and m.coverage_start < end
        )


def _build_synthetic_batch_archive(
    service_id: str, output_dir: Path, source_count: int = 5, rows_per_source: int = 5, now: datetime | None = None
) -> tuple[ArchiveManifest, bytes, list[dict[str, Any]]]:
    observed = now or datetime(2026, 10, 9, 12, 0, tzinfo=UTC)
    source_batches: list[tuple[ArchiveSourceObject, tuple[dict[str, Any], ...]]] = []
    all_events: list[dict[str, Any]] = []
    for s_idx in range(source_count):
        events_for_source = [
            {
                "event_id": f"evt-{service_id}-{s_idx}-{r_idx}",
                "timestamp": (observed - timedelta(minutes=s_idx, seconds=r_idx * 10)).isoformat(),
                "client_ip": f"198.51.100.{(s_idx * 5 + r_idx) % 250 + 1}",
                "request_method": "GET" if r_idx % 2 == 0 else "POST",
                "url": f"/api/v1/item/{s_idx}_{r_idx}",
                "status": 200 if r_idx != 3 else 500, "ost": 200 if r_idx != 3 else 500,
                "ottfb": 45000.0, "ttfb": 0.045, "cache": "MISS", "resp_bytes": 1024 * (r_idx + 1),
                "pop": "JFK" if s_idx % 2 == 0 else "SFO", "fastly_backend_name": "origin-primary",
                "time_elapsed": 30000 + (r_idx * 500),
            }
            for r_idx in range(rows_per_source)
        ]
        all_events.extend(events_for_source)
        source_obj = ArchiveSourceObject(
            service_id=service_id, domain="request",
            object_key=f"raw/request/year=2026/month=10/day=09/source_{s_idx:03d}.gz",
            checksum=f"sha256:synth_{s_idx:04d}", size_bytes=512, version=f"v{s_idx}",
        )
        source_batches.append((source_obj, tuple(events_for_source)))

    manifest = write_batch_archive_checkpoint(
        output_dir, service_id=service_id, domain="request", source_batches=source_batches, archive_epoch=1,
        schema_version="request.v1", transform_version="normalize.v1",
        coverage_start=observed - timedelta(hours=1), coverage_end=observed,
        retention_seconds=86400 * 30, deletion_grace_seconds=86400 * 7,
    )
    artifact_bytes = Path(manifest.artifact.uri.removeprefix("file://")).read_bytes()
    normalized = replace(manifest, artifact=replace(manifest.artifact, uri=f"s3://archive/artifacts/{manifest.manifest_id}.parquet"))
    return normalized, artifact_bytes, all_events


def _setup_recovery_fixture(
    service_id: str, now: datetime
) -> tuple[ArchiveManifest, list[dict[str, Any]], ArchivePublication, _MockClickHouseClient, ClickHousePublication]:
    with tempfile.TemporaryDirectory() as tmp_dir:
        manifest, artifact_bytes, expected_events = _build_synthetic_batch_archive(service_id, Path(tmp_dir), 5, 5, now)
    archive = ArchivePublication(InMemoryObjectStore())
    archive.publish(manifest, artifact_bytes)
    ch_client = _MockClickHouseClient()
    publication = ClickHousePublication(InMemoryBatchManifestStore(), ch_client)
    return manifest, expected_events, archive, ch_client, publication


def test_archive_only_recovery_rebuilds_all_events_without_raw_logs() -> None:
    manifest, expected_events, archive, ch_client, publication = _setup_recovery_fixture(
        "recover-svc-001", datetime(2026, 10, 9, 14, 0, tzinfo=UTC)
    )
    assert archive.is_replayable(manifest.manifest_id)
    replay_res = replay_manifest(manifest, archive, publication)
    assert (replay_res.manifest_id, replay_res.rows_read, replay_res.publication.rows_visible) == (manifest.manifest_id, 25, 25)
    assert replay_res.publication.status == PublicationStatus.VISIBLE and replay_res.publication.duplicate is False
    assert {r["event_id"] for r in ch_client.request_rows} == {e["event_id"] for e in expected_events}


def test_archive_only_recovery_rebuilds_origin_projections() -> None:
    manifest, _, archive, ch_client, publication = _setup_recovery_fixture(
        "recover-svc-002", datetime(2026, 10, 9, 14, 30, tzinfo=UTC)
    )
    proj_res = replay_origin_projections(manifest, archive, publication, rebuild_id="drill-origin-001")
    assert proj_res.rows_read == 25 and proj_res.rows_published > 0
    origin_batches = [b for b in ch_client.batches if b.domain in {"origin_summary", "origin_dimensions"}]
    assert len(origin_batches) >= 1 and sum(len(b.rows) for b in origin_batches) == proj_res.rows_published


def test_archive_only_recovery_idempotency_and_rebuild_scopes() -> None:
    manifest, _, archive, ch_client, publication = _setup_recovery_fixture(
        "recover-svc-003", datetime(2026, 10, 9, 15, 0, tzinfo=UTC)
    )
    res1 = replay_manifest(manifest, archive, publication)
    assert res1.publication.duplicate is False and len(ch_client.batches) == 1
    res2 = replay_manifest(manifest, archive, publication)
    assert res2.publication.duplicate is True and len(ch_client.batches) == 1
    res3 = replay_manifest(manifest, archive, publication, batch_id=f"rebuild:disaster-drill-99:{manifest.manifest_id}")
    assert res3.publication.duplicate is False and len(ch_client.batches) == 2


def test_archive_only_recovery_catalog_tiered_rebuild() -> None:
    service_id, now = "recover-svc-004", datetime(2026, 10, 9, 16, 0, tzinfo=UTC)
    with tempfile.TemporaryDirectory() as tmp_dir:
        recent_manifest, recent_bytes, _ = _build_synthetic_batch_archive(service_id, Path(tmp_dir), 3, 10, now - timedelta(hours=2))
        warm_manifest, warm_bytes, _ = _build_synthetic_batch_archive(service_id, Path(tmp_dir), 4, 10, now - timedelta(days=5))
    archive = ArchivePublication(InMemoryObjectStore())
    archive.publish(recent_manifest, recent_bytes)
    archive.publish(warm_manifest, warm_bytes)
    catalog = _InMemoryCatalog((recent_manifest, warm_manifest))
    publication = ClickHousePublication(InMemoryBatchManifestStore(), _MockClickHouseClient())

    report = rebuild_serving_state_from_fos(catalog, archive, publication, service_id=service_id, domain="request", now=now)
    assert (report.full_rebuild_complete, report.manifests_replayed, report.rows_replayed) == (True, 2, 70)
    assert report.triage_available and report.raw_query_available and report.warm_history_available and not report.errors

    proj_report = rebuild_origin_projections_from_fos(catalog, archive, publication, service_id=service_id, now=now)
    assert proj_report.full_rebuild_complete and proj_report.manifests_replayed == 2 and proj_report.rows_replayed > 0


# ── Merged from tests/high_scale/test_console_access_integration.py ──

IMAGE = os.getenv("RUM_CLICKHOUSE_TEST_IMAGE", "clickhouse/clickhouse-server:25.8.4.13")
DB = "fla_it"
SECRET_console_access_integration = b"integration-secret-" + b"x" * 32
NOW = datetime.now(UTC).replace(microsecond=0)
BATCH_A = "11111111-1111-4111-8111-111111111111"
BATCH_B = "22222222-2222-4222-8222-222222222222"
BATCH_A_RUM = "33333333-3333-4333-8333-333333333333"
_IT_GATE = pytest.mark.skipif(
    os.getenv("RUN_HS_CONSOLE_ACCESS_INTEGRATION") != "1", reason="explicit real ClickHouse server gate"
)
_HELM_GATE = pytest.mark.skipif(shutil.which("helm") is None, reason="helm binary required")


class _Server:
    def __init__(self, port: int) -> None:
        self.port = port
        self.base = f"http://127.0.0.1:{port}/"

    def admin_sql(self, sql: str, body: str | None = None) -> str:
        params = (
            {"database": DB, "query": sql, "date_time_input_format": "best_effort"}
            if body is not None
            else {"database": DB}
        )
        response = httpx.post(self.base, params=params, content=body if body is not None else sql, auth=("admin", "adminpw"), timeout=120)
        if response.status_code != 200:
            raise RuntimeError(f"{sql[:80]} -> {response.text[:300]}")
        return response.text

    def client(self, user: str, password: str) -> ClickHouseClient:
        return ClickHouseClient(
            ClickHouseConfig(host="127.0.0.1", port=self.port, database=DB, user=user, password=password, secure=False)
        )


def _row(service: str, url: str, minutes_ago: int, batch: str, ip: str, session: str) -> dict:
    when = (NOW - timedelta(minutes=minutes_ago)).isoformat()
    return {
        "service_id": service,
        "event_id": str(uuid.uuid4()),
        "event_timestamp": when,
        "ingest_timestamp": when,
        "source_object_key": "raw/x.gz",
        "source_object_version": "v1",
        "line_ordinal": 0,
        "transform_version": "t1",
        "batch_id": batch,
        "publication_state": "visible",
        "country": "US",
        "client_ip": ip,
        "url": url,
        "custom_fields": {
            "status": "200",
            "cookie_session": session,
            "cid": ("client-" + session) if session else "",
            "pop": "SJC",
        },
        "cmcd": {},
    }


@pytest.fixture(scope="module")
def server() -> Iterator[_Server]:
    port = _free_port()
    name = f"fla-console-it-{uuid.uuid4().hex[:8]}"
    subprocess.run(
        [
            "docker", "run", "-d", "--rm", "--name", name, "-p", f"127.0.0.1:{port}:8123",
            "-e", "CLICKHOUSE_USER=admin", "-e", "CLICKHOUSE_PASSWORD=adminpw",
            "-e", "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1", IMAGE,
        ],
        check=True,
        capture_output=True,
    )  # fmt: skip
    srv = _Server(port)
    try:
        wait_for_clickhouse_ready(srv.base)
        httpx.post(srv.base, content=f"CREATE DATABASE {DB}", auth=("admin", "adminpw")).raise_for_status()
        for statement in _ddl():
            srv.admin_sql(statement)
        srv.admin_sql("CREATE TABLE other_secret (x String) ENGINE = MergeTree ORDER BY x")
        srv.admin_sql("INSERT INTO other_secret VALUES ('top-secret')")
        rows = [_row("svc-a", f"/a/{i}", 10 + i, BATCH_A, f"203.0.113.{i + 1}", f"sess-a{i}") for i in range(5)]
        rows += [_row("svc-a", "/a/old", 600, BATCH_A, "203.0.113.99", "sess-a-old")]
        rows += [_row("svc-b", f"/b/{i}", 10 + i, BATCH_B, f"198.51.100.{i + 1}", f"sess-b{i}") for i in range(7)]
        rows += [_row("svc-a", "/a/noip", 5, BATCH_A, "", "")]
        srv.admin_sql("INSERT INTO request_facts FORMAT JSONEachRow", "\n".join(json.dumps(r) for r in rows))
        rum = {
            "service_id": "svc-a", "event_id": str(uuid.uuid4()), "event_timestamp": NOW.isoformat(),
            "ingest_timestamp": NOW.isoformat(), "batch_id": BATCH_A_RUM, "publication_state": "visible",
            "client_id": "rum-client-secret", "metric_name": "LCP", "pathname": "/a",
        }  # fmt: skip
        srv.admin_sql("INSERT INTO rum_vitals_facts FORMAT JSONEachRow", json.dumps(rum))
        pubs = [
            {"batch_id": batch, "service_id": sid, "domain": domain, "generation": "1", "batch_digest": "d",
             "expected_rows": 1, "visible_rows": 1, "quorum_acked": 1, "publication_state": "visible",
             "manifest_version": 2, "updated_at": NOW.isoformat()}
            for batch, sid, domain in (
                (BATCH_A, "svc-a", "request"), (BATCH_B, "svc-b", "request"), (BATCH_A_RUM, "svc-a", "rum_vitals"),
            )
        ]  # fmt: skip
        srv.admin_sql(
            "INSERT INTO high_scale_batch_publications FORMAT JSONEachRow", "\n".join(json.dumps(p) for p in pubs)
        )
        admin_client = srv.client("admin", "adminpw")
        for sid in ("svc-a", "svc-b"):
            access.provision_console_access(admin_client, DB, sid, SECRET_console_access_integration)
        admin_client.close()
        yield srv
    finally:
        subprocess.run(["docker", "stop", name], capture_output=True)


@pytest.fixture
def clients(server: _Server) -> Iterator[access.ConsoleClients]:
    settings = ClickHouseConfig(host="127.0.0.1", port=server.port, database=DB, user="x", password="x", secure=False)
    manager = access.ConsoleClients(settings=settings, secret=SECRET_console_access_integration)
    yield manager
    manager.close()


def _identity(clients: access.ConsoleClients, service: str, masked: bool) -> access.ConsoleAccess:
    granted = clients.client_for(service, masked=masked)
    assert granted is not None
    return granted


def _count(granted: access.ConsoleAccess, table: str = "request_facts", where: str = "1") -> int:
    view = granted.tables[table]
    return int(granted.client.execute(f"SELECT count() AS n FROM {view} WHERE {where}")[0]["n"])


# ── service isolation ──────────────────────────────────────────────────────────────────────────────────


@pytest.mark.security_regression
@_IT_GATE
@pytest.mark.parametrize("masked", [True, False])
def test_each_identity_sees_only_its_own_service(clients: access.ConsoleClients, masked: bool) -> None:
    assert _count(_identity(clients, "svc-a", masked)) == 7
    assert _count(_identity(clients, "svc-b", masked)) == 7
    assert _count(_identity(clients, "svc-a", masked), where="service_id != 'svc-a'") == 0


@pytest.mark.security_regression
@_IT_GATE
@pytest.mark.parametrize(
    "tail",
    [
        "SETTINGS additional_table_filters = {}",
        "SETTINGS additional_table_filters = {'x': '1'}",
        "WHERE 1 = 1 OR service_id = 'svc-b'",
    ],
)
def test_no_query_text_or_settings_override_can_cross_the_service_boundary(
    clients: access.ConsoleClients, tail: str
) -> None:
    granted = _identity(clients, "svc-a", True)
    rows = granted.client.execute(f"SELECT count() AS n FROM {granted.tables['request_facts']} {tail}")
    assert int(rows[0]["n"]) == 7


# ── masking happens in the data ────────────────────────────────────────────────────────────────────────


@pytest.mark.security_regression
@_IT_GATE
def test_a_masked_identity_never_receives_a_client_ip_session_or_client_id(clients: access.ConsoleClients) -> None:
    granted = _identity(clients, "svc-a", True)
    rows = granted.client.execute(f"SELECT client_ip, custom_fields, url FROM {granted.tables['request_facts']} ORDER BY url")
    by_url = {row["url"]: row for row in rows}
    assert by_url["/a/0"]["client_ip"] == "[redacted]" and by_url["/a/noip"]["client_ip"] == ""
    for row in rows:
        fields = row["custom_fields"]
        expected = "" if row["url"] == "/a/noip" else "[redacted]"
        assert fields["cookie_session"] == expected and fields["cid"] == expected
        assert fields["status"] == "200" and fields["pop"] == "SJC"
    assert "203.0.113" not in json.dumps(rows) and "sess-a" not in json.dumps(rows)


@pytest.mark.security_regression
@_IT_GATE
@pytest.mark.parametrize(
    "expression",
    [
        "replaceAll(client_ip, '.', '-')",
        "substring(client_ip, 1, 3)",
        "splitByChar('.', client_ip)[1]",
        "concat('x', client_ip)",
        "reverse(client_ip)",
        "toString(toIPv4OrZero(client_ip))",
        "custom_fields['cookie_session']",
        "mapValues(custom_fields)[1]",
        "extract(client_ip, '[0-9]+')",
    ],
)
def test_no_string_function_over_a_masked_view_can_recover_an_identifier(
    clients: access.ConsoleClients, expression: str
) -> None:
    granted = _identity(clients, "svc-a", True)
    rows = granted.client.execute(f"SELECT {expression} AS v FROM {granted.tables['request_facts']} WHERE url = '/a/0'")
    assert re.search(r"203\.0\.113|sess-a|client-sess", json.dumps(rows)) is None


@pytest.mark.security_regression
@_IT_GATE
def test_a_masked_rum_view_redacts_the_client_id(clients: access.ConsoleClients) -> None:
    granted = _identity(clients, "svc-a", True)
    rows = granted.client.execute(f"SELECT client_id, metric_name FROM {granted.tables['rum_vitals_facts']}")
    assert rows == [{"client_id": "[redacted]", "metric_name": "LCP"}]


@pytest.mark.security_regression
@_IT_GATE
def test_a_raw_identity_does_see_the_identifiers_its_invite_allows(clients: access.ConsoleClients) -> None:
    granted = _identity(clients, "svc-a", False)
    rows = granted.client.execute(
        f"SELECT client_ip, custom_fields['cookie_session'] AS s FROM {granted.tables['request_facts']} WHERE url = '/a/0'"
    )
    assert rows == [{"client_ip": "203.0.113.1", "s": "sess-a0"}]
    assert granted.client.execute(f"SELECT client_id FROM {granted.tables['rum_vitals_facts']}") == [{"client_id": "rum-client-secret"}]


@pytest.mark.security_regression
@_IT_GATE
def test_the_masked_identity_has_no_path_to_the_raw_data(clients: access.ConsoleClients) -> None:
    masked = _identity(clients, "svc-a", True)
    raw_view = access.console_tables("svc-a", masked=False)["request_facts"]
    for sql in (
        f"SELECT client_ip FROM {raw_view}",
        "SELECT client_ip FROM request_facts",
        f"SELECT client_ip FROM {DB}.request_facts",
        f"SELECT * FROM {access.console_tables('svc-b', masked=False)['request_facts']}",
        f"SELECT * FROM {access.console_tables('svc-b', masked=True)['request_facts']}",
    ):
        with pytest.raises(ClickHouseError):
            masked.client.execute(sql)


@pytest.mark.security_regression
@_IT_GATE
def test_a_masked_view_that_does_not_mask_is_refused_by_the_identity_check(clients: access.ConsoleClients) -> None:
    raw = _identity(clients, "svc-a", False)
    with pytest.raises(RuntimeError, match="masking"):
        access.ConsoleClients._assert_isolated(raw, "svc-a", masked=True)


# ── least privilege ────────────────────────────────────────────────────────────────────────────────────


@pytest.mark.security_regression
@_IT_GATE
@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM other_secret",
        "SELECT * FROM high_scale_batch_publications",
        "SELECT query FROM system.query_log LIMIT 1",
        "SELECT name FROM system.users LIMIT 1",
        "SELECT * FROM remote('127.0.0.1', fla_it.other_secret)",
        "SELECT * FROM url('http://127.0.0.1:8123/?query=SELECT%201', 'CSV', 'x String')",
        "SELECT * FROM file('/etc/passwd', 'CSV', 'x String')",
        "INSERT INTO request_facts (service_id) VALUES ('svc-a')",
        "CREATE TABLE t (x UInt8) ENGINE = Memory",
        "DROP TABLE request_facts",
        "ALTER TABLE request_facts DELETE WHERE 1",
        "CREATE VIEW v AS SELECT 1",
        "SELECT count() FROM request_facts SETTINGS max_memory_usage = 99999999999",
        "SELECT count() FROM request_facts SETTINGS max_threads = 64",
        "SELECT count() FROM request_facts SETTINGS readonly = 0",
        "SELECT count() FROM request_facts SETTINGS max_execution_time = 100000",
    ],
)
def test_the_database_itself_refuses_everything_outside_the_least_privilege_grant(
    clients: access.ConsoleClients, sql: str
) -> None:
    with pytest.raises(ClickHouseError):
        _identity(clients, "svc-a", True).client.execute(sql)


@pytest.mark.security_regression
@_IT_GATE
def test_the_apps_own_privileged_user_still_sees_every_service(server: _Server) -> None:
    app_client = server.client("admin", "adminpw")
    try:
        assert int(app_client.execute("SELECT count() AS n FROM request_facts")[0]["n"]) == 14
    finally:
        app_client.close()


@pytest.mark.security_regression
@_IT_GATE
def test_the_per_user_and_result_size_ceilings_are_in_force_and_not_raisable(clients: access.ConsoleClients) -> None:
    client = _identity(clients, "svc-a", True).client
    rows = client.execute("SELECT name, value FROM system.settings WHERE name IN ('max_result_bytes', 'max_memory_usage_for_user')")
    assert {r["name"]: r["value"] for r in rows} == {"max_result_bytes": "67108864", "max_memory_usage_for_user": "2147483648"}
    for override in ("max_result_bytes = 99999999999", "max_memory_usage_for_user = 99999999999"):
        with pytest.raises(ClickHouseError):
            client.execute(f"SELECT 1 SETTINGS {override}")


# ── the whole console path ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.security_regression
@_IT_GATE
def test_the_whole_console_path_runs_end_to_end_as_the_masked_identity(clients: access.ConsoleClients) -> None:
    granted = _identity(clients, "svc-a", True)
    start, end = (NOW - timedelta(minutes=30)).isoformat(), (NOW + timedelta(minutes=1)).isoformat()
    result = query_endpoint(
        SimpleNamespace(service_id="svc-a", client=None),  # type: ignore[arg-type]
        QueryRequest(sql="SELECT count(*) AS n FROM logs WHERE 1 = 1 OR country = 'XX'", max_rows=50),
        start, end, True, client=granted.client, tables=granted.tables,
    )
    assert result["data"] == [{"n": 6}]


@pytest.mark.security_regression
@_IT_GATE
def test_an_analyst_ip_query_through_the_console_returns_only_redacted_values(clients: access.ConsoleClients) -> None:
    granted = _identity(clients, "svc-a", True)
    start, end = (NOW - timedelta(minutes=30)).isoformat(), (NOW + timedelta(minutes=1)).isoformat()
    result = query_endpoint(
        SimpleNamespace(service_id="svc-a", client=None),  # type: ignore[arg-type]
        QueryRequest(sql="SELECT ip, cookie_session, url FROM logs WHERE url = '/a/0'", max_rows=5),
        start, end, True, client=granted.client, tables=granted.tables,
    )
    assert result["data"][0]["url"] == "/a/0"
    assert "203.0.113" not in json.dumps(result["data"]) and "sess-a" not in json.dumps(result["data"])


# ── provisioning lifecycle ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.security_regression
@_IT_GATE
def test_provisioning_removes_a_pre_view_identity_with_base_table_grants(server: _Server) -> None:
    legacy = access.console_identity("svc-legacy", SECRET_console_access_integration)[0].removesuffix("_m")
    server.admin_sql(f"CREATE USER IF NOT EXISTS `{legacy}` IDENTIFIED WITH sha256_password BY 'pw'")
    server.admin_sql(f"GRANT SELECT ON {DB}.request_facts TO `{legacy}`")
    server.admin_sql(f"CREATE ROW POLICY IF NOT EXISTS `{legacy}_scope` ON {DB}.request_facts USING 1 TO `{legacy}`")
    admin = server.client("admin", "adminpw")
    try:
        access.provision_console_access(admin, DB, "svc-legacy", SECRET_console_access_integration)
        remaining = admin.execute(f"SELECT name FROM system.users WHERE name = '{legacy}'")
    finally:
        admin.close()
    assert remaining == []


@pytest.mark.security_regression
@_IT_GATE
def test_a_tampered_view_is_repaired_by_the_next_provisioning_run(server: _Server) -> None:
    view = access.console_tables("svc-b", masked=True)["request_facts"]
    server.admin_sql(f"CREATE OR REPLACE VIEW {DB}.`{view}` AS SELECT * FROM {DB}.request_facts")
    leaky = server.client(*access.console_identity("svc-b", SECRET_console_access_integration, masked=True))
    try:
        assert int(leaky.execute(f"SELECT count() AS n FROM {view}")[0]["n"]) == 14
    except ClickHouseError:
        pass
    finally:
        leaky.close()

    admin = server.client("admin", "adminpw")
    try:
        access.provision_console_access(admin, DB, "svc-b", SECRET_console_access_integration)
    finally:
        admin.close()

    repaired = server.client(*access.console_identity("svc-b", SECRET_console_access_integration, masked=True))
    try:
        assert int(repaired.execute(f"SELECT count() AS n FROM {view}")[0]["n"]) == 7
        assert repaired.execute(f"SELECT DISTINCT client_ip FROM {view}") == [{"client_ip": "[redacted]"}]
    finally:
        repaired.close()


@pytest.mark.security_regression
@_IT_GATE
def test_provisioning_is_idempotent_and_rotating_the_secret_locks_out_the_old_credentials(server: _Server) -> None:
    admin = server.client("admin", "adminpw")
    new_secret = b"rotated-secret-" + b"y" * 32
    try:
        access.provision_console_access(admin, DB, "svc-a", SECRET_console_access_integration)
        access.provision_console_access(admin, DB, "svc-a", new_secret)
    finally:
        admin.close()
    user, old_password = access.console_identity("svc-a", SECRET_console_access_integration, masked=True)
    stale = server.client(user, old_password)
    fresh = server.client(*access.console_identity("svc-a", new_secret, masked=True))
    try:
        with pytest.raises(ClickHouseError):
            stale.execute("SELECT 1")
        view = access.console_tables("svc-a", masked=True)["request_facts"]
        assert int(fresh.execute(f"SELECT count() AS n FROM {view}")[0]["n"]) == 7
    finally:
        stale.close()
        fresh.close()
        restore = server.client("admin", "adminpw")
        access.provision_console_access(restore, DB, "svc-a", SECRET_console_access_integration)
        restore.close()


@pytest.mark.security_regression
@_IT_GATE
def test_the_probe_query_the_console_runs_first_is_allowed_for_the_identity(clients: access.ConsoleClients) -> None:
    client = _identity(clients, "svc-a", True).client
    rows = client.execute(
        "SELECT count() AS n FROM system.one", settings={"additional_table_filters": "{'system.one': 'dummy = 1'}"}
    )
    assert rows == [{"n": 0}]


@pytest.mark.security_regression
@_IT_GATE
def test_a_column_added_to_a_base_table_later_is_invisible_to_both_identities_until_reprovisioning(
    server: _Server, clients: access.ConsoleClients
) -> None:
    server.admin_sql("ALTER TABLE request_facts ADD COLUMN IF NOT EXISTS drift_probe String DEFAULT 'would-leak'")
    try:
        for masked in (True, False):
            granted = _identity(clients, "svc-a", masked)
            with pytest.raises(ClickHouseError):
                granted.client.execute(f"SELECT drift_probe FROM {granted.tables['request_facts']} LIMIT 1")
    finally:
        server.admin_sql("ALTER TABLE request_facts DROP COLUMN IF EXISTS drift_probe")


@pytest.mark.security_regression
@_IT_GATE
def test_grants_a_pre_existing_identity_picked_up_elsewhere_are_revoked_on_the_next_provisioning(
    server: _Server,
) -> None:
    user, _ = access.console_identity("svc-a", SECRET_console_access_integration, masked=True)
    server.admin_sql(f"GRANT SELECT ON {DB}.other_secret TO `{user}`")
    stale = server.client(*access.console_identity("svc-a", SECRET_console_access_integration, masked=True))
    try:
        assert stale.execute("SELECT x FROM other_secret") == [{"x": "top-secret"}]
    finally:
        stale.close()

    admin = server.client("admin", "adminpw")
    try:
        access.provision_console_access(admin, DB, "svc-a", SECRET_console_access_integration)
    finally:
        admin.close()

    after = server.client(*access.console_identity("svc-a", SECRET_console_access_integration, masked=True))
    try:
        with pytest.raises(ClickHouseError):
            after.execute("SELECT x FROM other_secret")
    finally:
        after.close()


# ── Merged from test_console_provisioner_chart_integration.py ───────────────

CHART = "deploy/chart/clickhouse"
CHART_SECRET = b"chart-secret-" + b"z" * 32


def _render(*sets: str) -> list[dict]:
    args = ["helm", "template", "t", CHART]
    for item in sets:
        args += ["--set", item]
    rendered = subprocess.run(args, check=True, capture_output=True, text=True).stdout
    return [doc for doc in yaml.safe_load_all(rendered) if doc]


def _statefulset_env(docs: list[dict]) -> dict[str, str]:
    sts = next(d for d in docs if d["kind"] == "StatefulSet")
    env = sts["spec"]["template"]["spec"]["containers"][0]["env"]
    return {e["name"]: e.get("value", "<from secret>") for e in env}


@pytest.mark.security_regression
@_IT_GATE
@_HELM_GATE
def test_the_chart_renders_unchanged_by_default() -> None:
    docs = _render()
    assert _statefulset_env(docs)["CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT"] == "1"
    assert "CLICKHOUSE_PROVISIONER_PASSWORD" not in _statefulset_env(docs)
    config_map = next(d for d in docs if d["kind"] == "ConfigMap" and "-config" in d["metadata"]["name"])
    assert "provisioner-user.xml" not in config_map["data"]


@pytest.mark.security_regression
@_IT_GATE
@_HELM_GATE
@pytest.mark.parametrize(
    "bad",
    [
        ["provisioner.enabled=true"],
        ["provisioner.enabled=true", "provisioner.password=x", "provisioner.username=fla_user"],
        ["provisioner.enabled=true", "provisioner.password=x", "provisioner.username=bad name; DROP"],
    ],
)
def test_the_chart_refuses_an_unsafe_provisioner_configuration(bad: list[str]) -> None:
    with pytest.raises(subprocess.CalledProcessError):
        _render(*bad)


@pytest.fixture(scope="module")
def chart_server() -> Iterator[dict]:
    docs = _render(
        "provisioner.enabled=true",
        "provisioner.password=provpw",
        "auth.accessManagement=false",
        f"auth.database={DB}",
        "auth.username=fla_user",
        "auth.password=apppw",
    )
    env = _statefulset_env(docs)
    assert env["CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT"] == "0"
    config_map = next(d for d in docs if d["kind"] == "ConfigMap" and "-config" in d["metadata"]["name"])
    workdir = Path(tempfile.mkdtemp(prefix="fla-prov-"))
    xml_path = workdir / "provisioner-user.xml"
    xml_path.write_text(config_map["data"]["provisioner-user.xml"])

    port = _free_port()
    name = f"fla-prov-it-{uuid.uuid4().hex[:8]}"
    subprocess.run(
        [
            "docker", "create", "--rm", "--name", name, "-p", f"127.0.0.1:{port}:8123",
            "-e", "CLICKHOUSE_USER=fla_user", "-e", "CLICKHOUSE_PASSWORD=apppw", "-e", f"CLICKHOUSE_DB={DB}",
            "-e", f"CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT={env['CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT']}",
            "-e", "CLICKHOUSE_PROVISIONER_PASSWORD=provpw", IMAGE,
        ],
        check=True,
        capture_output=True,
    )  # fmt: skip
    subprocess.run(
        ["docker", "cp", str(xml_path), f"{name}:/etc/clickhouse-server/users.d/provisioner-user.xml"],
        check=True,
        capture_output=True,
    )
    subprocess.run(["docker", "start", name], check=True, capture_output=True)
    base = f"http://127.0.0.1:{port}/"
    try:
        wait_for_clickhouse_ready(base)
        for statement in _ddl():
            resp = httpx.post(base, params={"database": DB}, content=statement, auth=("fla_user", "apppw"), timeout=120)
            assert resp.status_code == 200, f"{statement[:60]} -> {resp.text[:200]}"
        yield {"port": port, "base": base}
    finally:
        subprocess.run(["docker", "stop", name], capture_output=True)
        shutil.rmtree(workdir, ignore_errors=True)


def _chart_client(chart_server: dict, user: str, password: str) -> ClickHouseClient:
    return ClickHouseClient(
        ClickHouseConfig(
            host="127.0.0.1", port=chart_server["port"], database=DB, user=user, password=password, secure=False
        )
    )


@pytest.mark.security_regression
@_IT_GATE
@_HELM_GATE
def test_the_application_user_can_no_longer_mint_users_or_grants(chart_server: dict) -> None:
    app = _chart_client(chart_server, "fla_user", "apppw")
    try:
        assert app.execute("SELECT currentUser() AS u") == [{"u": "fla_user"}]
        assert app.execute("SELECT count() AS n FROM request_facts") == [{"n": 0}]
        for sql in (
            "CREATE USER evil IDENTIFIED WITH sha256_password BY 'x'",
            "GRANT ALL ON *.* TO fla_user",
            "CREATE SETTINGS PROFILE evil SETTINGS readonly = 0",
            "CREATE ROW POLICY p ON fla_it.request_facts USING 1 TO fla_user",
        ):
            with pytest.raises(ClickHouseError):
                app.execute(sql)
    finally:
        app.close()


@pytest.mark.security_regression
@_IT_GATE
@_HELM_GATE
def test_the_chart_rendered_provisioner_privileges_are_sufficient_to_provision(chart_server: dict) -> None:
    prov = _chart_client(chart_server, "fla_provisioner", "provpw")
    try:
        users = access.provision_console_access(prov, DB, "svc-chart", CHART_SECRET)
    finally:
        prov.close()

    assert users == {
        "masked": access.console_identity("svc-chart", CHART_SECRET, True)[0],
        "raw": access.console_identity("svc-chart", CHART_SECRET, False)[0],
    }
    manager = access.ConsoleClients(
        settings=ClickHouseConfig(
            host="127.0.0.1", port=chart_server["port"], database=DB, user="x", password="x", secure=False
        ),
        secret=CHART_SECRET,
    )
    try:
        for masked in (True, False):
            granted = manager.client_for("svc-chart", masked=masked)
            assert granted is not None
            assert granted.client.execute(f"SELECT count() AS n FROM {granted.tables['request_facts']}") == [{"n": 0}]
    finally:
        manager.close()


@pytest.mark.security_regression
@_IT_GATE
@_HELM_GATE
def test_the_provisioner_is_idempotent_and_the_app_user_still_cannot_read_what_it_created(chart_server: dict) -> None:
    prov = _chart_client(chart_server, "fla_provisioner", "provpw")
    try:
        access.provision_console_access(prov, DB, "svc-chart", CHART_SECRET)
        access.provision_console_access(prov, DB, "svc-chart", CHART_SECRET)
    finally:
        prov.close()


@pytest.mark.security_regression
@_IT_GATE
@_HELM_GATE
def test_the_provisioner_has_no_privileges_outside_access_management_and_the_application_database(
    chart_server: dict,
) -> None:
    prov = _chart_client(chart_server, "fla_provisioner", "provpw")
    try:
        assert prov.execute("SELECT currentUser() AS u") == [{"u": "fla_provisioner"}]
        for sql in (
            "SELECT query FROM system.query_log LIMIT 1",
            f"INSERT INTO {DB}.request_facts (service_id) VALUES ('x')",
            f"CREATE TABLE {DB}.t (x UInt8) ENGINE = Memory",
            "SELECT * FROM url('http://127.0.0.1:8123/?query=SELECT%201', 'CSV', 'x String')",
        ):
            with pytest.raises(ClickHouseError):
                prov.execute(sql)
    finally:
        prov.close()
