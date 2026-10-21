"""Opt-in gate against a REAL ClickHouse server (not ``clickhouse local``): users, grants, views and profiles.

RUN_HS_CONSOLE_ACCESS_INTEGRATION=1 uv run pytest tests/high_scale/test_console_access_integration.py

Starts a disposable ``clickhouse-server`` container on a loopback port, provisions per-service, per-mask-mode
console identities with the production code, and drives every assertion through the production
``ClickHouseClient`` so the exact settings the app sends are exercised. This is the test that proves the
database, not the SQL guard, holds the boundary: the attacks below deliberately bypass the guard and talk to
ClickHouse as the analyst's identity.
"""

from __future__ import annotations

import json
import os
import re
import socket
import subprocess
import time
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from backend.config import ClickHouseConfig
from backend.core.clickhouse_client import ClickHouseClient, ClickHouseError
from backend.high_scale import console_access as access
from backend.high_scale.query import query_endpoint
from backend.models.query import QueryRequest

pytestmark = [
    pytest.mark.security_regression,
    pytest.mark.skipif(
        os.getenv("RUN_HS_CONSOLE_ACCESS_INTEGRATION") != "1", reason="explicit real ClickHouse server gate"
    ),
]

IMAGE = os.getenv("RUM_CLICKHOUSE_TEST_IMAGE", "clickhouse/clickhouse-server:25.8.4.13")
DB = "fla_it"
SECRET = b"integration-secret-" + b"x" * 32
SQL_ROOT = Path("backend/high_scale/sql")
NOW = datetime.now(UTC).replace(microsecond=0)
BATCH_A = "11111111-1111-4111-8111-111111111111"
BATCH_B = "22222222-2222-4222-8222-222222222222"
BATCH_A_RUM = "33333333-3333-4333-8333-333333333333"


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _ddl() -> list[str]:
    statements: list[str] = []
    for name in ("publication_schema.sql", "request_schema.sql", "rum_schema.sql"):
        text = (SQL_ROOT / name).read_text()
        text = re.sub(
            r"ReplicatedReplacingMergeTree\(([^)]+)\)\s*\('[^']+',\s*'[^']+'\)", r"ReplacingMergeTree(\1)", text
        )
        text = re.sub(r"ReplicatedMergeTree\([^;]*?\)", "MergeTree()", text)
        text = re.sub(r"\nTTL [^;]*;", ";", text)
        text = re.sub(r"--[^\n]*", "", text)
        statements += [s.strip() for s in text.split(";") if s.strip()]
    return statements


class _Server:
    def __init__(self, port: int) -> None:
        self.port = port
        self.base = f"http://127.0.0.1:{port}/"

    def admin_sql(self, sql: str, body: str | None = None) -> str:
        response = httpx.post(
            self.base,
            params=(
                {"database": DB, "query": sql, "date_time_input_format": "best_effort"}
                if body is not None
                else {"database": DB}
            ),
            content=body if body is not None else sql,
            auth=("admin", "adminpw"),
            timeout=120,
        )
        if response.status_code != 200:
            raise RuntimeError(f"{sql[:80]} -> {response.text[:300]}")
        return response.text

    def client(self, user: str, password: str) -> ClickHouseClient:
        return ClickHouseClient(
            ClickHouseConfig(host="127.0.0.1", port=self.port, database=DB, user=user, password=password, secure=False)
        )


def _row(service: str, url: str, minutes_ago: int, batch: str, ip: str, session: str) -> dict:
    when = NOW - timedelta(minutes=minutes_ago)
    return {
        "service_id": service,
        "event_id": str(uuid.uuid4()),
        "event_timestamp": when.isoformat(),
        "ingest_timestamp": when.isoformat(),
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
        deadline = time.time() + 90
        while time.time() < deadline:
            try:
                if httpx.get(f"{srv.base}ping", timeout=2).text.strip() == "Ok.":
                    break
            except httpx.HTTPError:
                pass
            time.sleep(1)
        else:
            raise RuntimeError("clickhouse-server did not become ready")
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
            access.provision_console_access(admin_client, DB, sid, SECRET)
        admin_client.close()
        yield srv
    finally:
        subprocess.run(["docker", "stop", name], capture_output=True)


@pytest.fixture
def clients(server: _Server) -> Iterator[access.ConsoleClients]:
    settings = ClickHouseConfig(host="127.0.0.1", port=server.port, database=DB, user="x", password="x", secure=False)
    manager = access.ConsoleClients(settings=settings, secret=SECRET)
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


@pytest.mark.parametrize("masked", [True, False])
def test_each_identity_sees_only_its_own_service(clients: access.ConsoleClients, masked: bool) -> None:
    assert _count(_identity(clients, "svc-a", masked)) == 7  # 5 recent + 1 old + 1 without an ip
    assert _count(_identity(clients, "svc-b", masked)) == 7
    assert _count(_identity(clients, "svc-a", masked), where="service_id != 'svc-a'") == 0


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
    # Talks to ClickHouse directly as the analyst's identity: the SQL guard is deliberately bypassed.
    granted = _identity(clients, "svc-a", True)

    rows = granted.client.execute(f"SELECT count() AS n FROM {granted.tables['request_facts']} {tail}")

    assert int(rows[0]["n"]) == 7


# ── masking happens in the data ────────────────────────────────────────────────────────────────────────


def test_a_masked_identity_never_receives_a_client_ip_session_or_client_id(clients: access.ConsoleClients) -> None:
    granted = _identity(clients, "svc-a", True)
    view = granted.tables["request_facts"]

    rows = granted.client.execute(f"SELECT client_ip, custom_fields, url FROM {view} ORDER BY url")

    by_url = {row["url"]: row for row in rows}
    assert by_url["/a/0"]["client_ip"] == "[redacted]"
    assert by_url["/a/noip"]["client_ip"] == "", "empty stays empty, as in the standard console"
    for row in rows:
        fields = row["custom_fields"]
        if row["url"] == "/a/noip":
            assert fields["cookie_session"] == "" and fields["cid"] == ""
        else:
            assert fields["cookie_session"] == "[redacted]" and fields["cid"] == "[redacted]"
        assert fields["status"] == "200" and fields["pop"] == "SJC", "non-sensitive keys are kept"
    assert "203.0.113" not in json.dumps(rows) and "sess-a" not in json.dumps(rows)


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


def test_a_masked_rum_view_redacts_the_client_id(clients: access.ConsoleClients) -> None:
    granted = _identity(clients, "svc-a", True)

    rows = granted.client.execute(f"SELECT client_id, metric_name FROM {granted.tables['rum_vitals_facts']}")

    assert rows == [{"client_id": "[redacted]", "metric_name": "LCP"}]


def test_a_raw_identity_does_see_the_identifiers_its_invite_allows(clients: access.ConsoleClients) -> None:
    granted = _identity(clients, "svc-a", False)

    rows = granted.client.execute(
        f"SELECT client_ip, custom_fields['cookie_session'] AS s FROM {granted.tables['request_facts']} "
        "WHERE url = '/a/0'"
    )

    assert rows == [{"client_ip": "203.0.113.1", "s": "sess-a0"}]
    rum = granted.client.execute(f"SELECT client_id FROM {granted.tables['rum_vitals_facts']}")
    assert rum == [{"client_id": "rum-client-secret"}]


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


def test_a_masked_view_that_does_not_mask_is_refused_by_the_identity_check(clients: access.ConsoleClients) -> None:
    raw = _identity(clients, "svc-a", False)

    with pytest.raises(RuntimeError, match="masking"):
        access.ConsoleClients._assert_isolated(raw, "svc-a", masked=True)


# ── least privilege ────────────────────────────────────────────────────────────────────────────────────


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


def test_the_apps_own_privileged_user_still_sees_every_service(server: _Server) -> None:
    app_client = server.client("admin", "adminpw")
    try:
        assert int(app_client.execute("SELECT count() AS n FROM request_facts")[0]["n"]) == 14
    finally:
        app_client.close()


def test_the_per_user_and_result_size_ceilings_are_in_force_and_not_raisable(clients: access.ConsoleClients) -> None:
    client = _identity(clients, "svc-a", True).client

    limits = {
        r["name"]: r["value"]
        for r in client.execute(
            "SELECT name, value FROM system.settings WHERE name IN ('max_result_bytes', 'max_memory_usage_for_user')"
        )
    }

    assert limits == {"max_result_bytes": "67108864", "max_memory_usage_for_user": "2147483648"}
    for override in ("max_result_bytes = 99999999999", "max_memory_usage_for_user = 99999999999"):
        with pytest.raises(ClickHouseError):
            client.execute(f"SELECT 1 SETTINGS {override}")


# ── the whole console path ─────────────────────────────────────────────────────────────────────────────


def test_the_whole_console_path_runs_end_to_end_as_the_masked_identity(clients: access.ConsoleClients) -> None:
    granted = _identity(clients, "svc-a", True)
    start, end = (NOW - timedelta(minutes=30)).isoformat(), (NOW + timedelta(minutes=1)).isoformat()

    result = query_endpoint(
        SimpleNamespace(service_id="svc-a", client=None),  # type: ignore[arg-type]
        QueryRequest(sql="SELECT count(*) AS n FROM logs WHERE 1 = 1 OR country = 'XX'", max_rows=50),
        start,
        end,
        True,
        client=granted.client,
        tables=granted.tables,
    )

    # 6 svc-a rows inside the window (5 recent + the one without an ip): not svc-b's 7, not the 600-minute-old row.
    assert result["data"] == [{"n": 6}]


def test_an_analyst_ip_query_through_the_console_returns_only_redacted_values(clients: access.ConsoleClients) -> None:
    granted = _identity(clients, "svc-a", True)
    start, end = (NOW - timedelta(minutes=30)).isoformat(), (NOW + timedelta(minutes=1)).isoformat()

    result = query_endpoint(
        SimpleNamespace(service_id="svc-a", client=None),  # type: ignore[arg-type]
        QueryRequest(sql="SELECT ip, cookie_session, url FROM logs WHERE url = '/a/0'", max_rows=5),
        start,
        end,
        True,
        client=granted.client,
        tables=granted.tables,
    )

    row = result["data"][0]
    assert row["url"] == "/a/0"
    assert "203.0.113" not in json.dumps(result["data"]) and "sess-a" not in json.dumps(result["data"])


# ── provisioning lifecycle ─────────────────────────────────────────────────────────────────────────────


def test_provisioning_removes_a_pre_view_identity_with_base_table_grants(server: _Server) -> None:
    legacy = access.console_identity("svc-legacy", SECRET)[0].removesuffix("_m")
    server.admin_sql(f"CREATE USER IF NOT EXISTS `{legacy}` IDENTIFIED WITH sha256_password BY 'pw'")
    server.admin_sql(f"GRANT SELECT ON {DB}.request_facts TO `{legacy}`")
    server.admin_sql(f"CREATE ROW POLICY IF NOT EXISTS `{legacy}_scope` ON {DB}.request_facts USING 1 TO `{legacy}`")

    admin = server.client("admin", "adminpw")
    try:
        access.provision_console_access(admin, DB, "svc-legacy", SECRET)
        remaining = admin.execute(f"SELECT name FROM system.users WHERE name = '{legacy}'")
    finally:
        admin.close()

    assert remaining == []


def test_a_tampered_view_is_repaired_by_the_next_provisioning_run(server: _Server) -> None:
    view = access.console_tables("svc-b", masked=True)["request_facts"]
    server.admin_sql(f"CREATE OR REPLACE VIEW {DB}.`{view}` AS SELECT * FROM {DB}.request_facts")  # silently leaky
    leaky = server.client(*access.console_identity("svc-b", SECRET, masked=True))
    try:
        assert int(leaky.execute(f"SELECT count() AS n FROM {view}")[0]["n"]) == 14, "sanity: the tamper leaks"
    except ClickHouseError:
        pass  # a non-definer view is denied for lack of base-table access: also safe, and repaired below
    finally:
        leaky.close()

    admin = server.client("admin", "adminpw")
    try:
        access.provision_console_access(admin, DB, "svc-b", SECRET)
    finally:
        admin.close()

    repaired = server.client(*access.console_identity("svc-b", SECRET, masked=True))
    try:
        assert int(repaired.execute(f"SELECT count() AS n FROM {view}")[0]["n"]) == 7
        ips = repaired.execute(f"SELECT DISTINCT client_ip FROM {view}")
        assert ips == [{"client_ip": "[redacted]"}]
    finally:
        repaired.close()


def test_provisioning_is_idempotent_and_rotating_the_secret_locks_out_the_old_credentials(server: _Server) -> None:
    admin = server.client("admin", "adminpw")
    new_secret = b"rotated-secret-" + b"y" * 32
    try:
        access.provision_console_access(admin, DB, "svc-a", SECRET)  # second run: no error
        access.provision_console_access(admin, DB, "svc-a", new_secret)
    finally:
        admin.close()
    user, old_password = access.console_identity("svc-a", SECRET, masked=True)
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
        access.provision_console_access(restore, DB, "svc-a", SECRET)  # later tests still authenticate
        restore.close()


def test_the_probe_query_the_console_runs_first_is_allowed_for_the_identity(clients: access.ConsoleClients) -> None:
    client = _identity(clients, "svc-a", True).client

    rows = client.execute(
        "SELECT count() AS n FROM system.one", settings={"additional_table_filters": "{'system.one': 'dummy = 1'}"}
    )

    assert rows == [{"n": 0}]
