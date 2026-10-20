"""Opt-in gate against a REAL ClickHouse server (not ``clickhouse local``): users, grants, profiles and row policies.

RUN_HS_CONSOLE_ACCESS_INTEGRATION=1 uv run pytest tests/high_scale/test_console_access_integration.py

Starts a disposable ``clickhouse-server`` container on a loopback port, provisions per-service console identities
with the production code, and drives every assertion through the production ``ClickHouseClient`` so the exact
settings the app sends are exercised. This is the test that proves the database, not the SQL guard, holds the
boundary: the attacks below deliberately bypass the guard and talk to ClickHouse as the analyst's identity.
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


def _row(service: str, url: str, minutes_ago: int, batch: str) -> dict:
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
        "client_ip": "203.0.113.5",
        "url": url,
        "custom_fields": {"status": "200"},
        "cmcd": {},
    }


BATCH_A = "11111111-1111-4111-8111-111111111111"
BATCH_B = "22222222-2222-4222-8222-222222222222"


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
        rows = [_row("svc-a", f"/a/{i}", 10 + i, BATCH_A) for i in range(5)]
        rows += [_row("svc-a", "/a/old", 600, BATCH_A)]
        rows += [_row("svc-b", f"/b/{i}", 10 + i, BATCH_B) for i in range(7)]
        srv.admin_sql(f"INSERT INTO request_facts FORMAT JSONEachRow", "\n".join(json.dumps(r) for r in rows))  # noqa: F541
        pubs = [
            {"batch_id": batch, "service_id": sid, "domain": "request", "generation": "1", "batch_digest": "d",
             "expected_rows": 1, "visible_rows": 1, "quorum_acked": 1, "publication_state": "visible",
             "manifest_version": 2, "updated_at": NOW.isoformat()}
            for batch, sid in ((BATCH_A, "svc-a"), (BATCH_B, "svc-b"))
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


def _count(client: ClickHouseClient, sql: str = "SELECT count() AS n FROM request_facts", **kw) -> int:
    return int(client.execute(sql, **kw)[0]["n"])


def test_each_identity_sees_only_its_own_service(clients: access.ConsoleClients) -> None:
    assert _count(clients.client_for("svc-a")) == 6
    assert _count(clients.client_for("svc-b")) == 7


@pytest.mark.parametrize(
    "tail",
    [
        "SETTINGS additional_table_filters = {}",
        "SETTINGS additional_table_filters = {'request_facts': '1'}",
        "WHERE 1 = 1 OR service_id = 'svc-b'",
    ],
)
def test_no_query_text_or_settings_override_can_cross_the_service_boundary(
    clients: access.ConsoleClients, tail: str
) -> None:
    # Talks to ClickHouse directly as the analyst's identity: the SQL guard is deliberately bypassed.
    assert _count(clients.client_for("svc-a"), f"SELECT count() AS n FROM request_facts {tail}") == 6


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM other_secret",
        "SELECT query FROM system.query_log LIMIT 1",
        "SELECT * FROM remote('127.0.0.1', fla_it.other_secret)",
        "SELECT * FROM url('http://127.0.0.1:8123/?query=SELECT%201', 'CSV', 'x String')",
        "SELECT * FROM file('/etc/passwd', 'CSV', 'x String')",
        "INSERT INTO request_facts (service_id) VALUES ('svc-a')",
        "CREATE TABLE t (x UInt8) ENGINE = Memory",
        "DROP TABLE request_facts",
        "ALTER TABLE request_facts DELETE WHERE 1",
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
        clients.client_for("svc-a").execute(sql)


def test_the_user_cannot_read_the_other_services_publication_rows(clients: access.ConsoleClients) -> None:
    client = clients.client_for("svc-a")

    assert _count(client, "SELECT count() AS n FROM high_scale_batch_publications") == 1
    assert _count(client, "SELECT count() AS n FROM high_scale_batch_publications WHERE service_id = 'svc-b'") == 0


def test_the_apps_own_privileged_user_still_sees_every_service(server: _Server) -> None:
    app_client = server.client("admin", "adminpw")
    try:
        assert _count(app_client) == 13
    finally:
        app_client.close()


def test_the_whole_console_path_runs_end_to_end_as_the_analyst_identity(clients: access.ConsoleClients) -> None:
    client = clients.client_for("svc-a")
    start, end = (NOW - timedelta(minutes=30)).isoformat(), (NOW + timedelta(minutes=1)).isoformat()

    result = query_endpoint(
        SimpleNamespace(service_id="svc-a", client=None),  # type: ignore[arg-type]
        QueryRequest(sql="SELECT count(*) AS n FROM logs WHERE 1 = 1 OR country = 'XX'", max_rows=50),
        start,
        end,
        False,
        client=client,
    )

    # 5 recent svc-a rows: not svc-b's 7, and not the 600-minute-old svc-a row outside the analyst window.
    assert result["data"] == [{"n": 5}]


def test_an_identity_without_a_row_policy_is_refused_before_it_can_serve_a_query(server: _Server) -> None:
    server.admin_sql("CREATE USER IF NOT EXISTS leaky IDENTIFIED WITH sha256_password BY 'pw' DEFAULT DATABASE fla_it")
    server.admin_sql(f"GRANT SELECT ON {DB}.request_facts TO leaky")
    leaky = server.client("leaky", "pw")
    try:
        assert _count(leaky) == 13, "sanity: a grant without a policy would fail open"
        with pytest.raises(RuntimeError, match="isolation"):
            access.ConsoleClients._assert_isolated(leaky, "svc-a")
    finally:
        leaky.close()


def test_provisioning_is_idempotent_and_rotating_the_secret_locks_out_the_old_credentials(server: _Server) -> None:
    admin = server.client("admin", "adminpw")
    try:
        access.provision_console_access(admin, DB, "svc-a", SECRET)  # second run: no error, no duplicate policy
        new_secret = b"rotated-secret-" + b"y" * 32
        access.provision_console_access(admin, DB, "svc-a", new_secret)
    finally:
        admin.close()
    user, old_password = access.console_identity("svc-a", SECRET)
    stale = server.client(user, old_password)
    fresh = server.client(*(access.console_identity("svc-a", new_secret)))
    try:
        with pytest.raises(ClickHouseError):
            stale.execute("SELECT 1")
        assert _count(fresh) == 6
    finally:
        stale.close()
        fresh.close()
        # restore the original secret so later tests in this module still authenticate
        restore = server.client("admin", "adminpw")
        access.provision_console_access(restore, DB, "svc-a", SECRET)
        restore.close()


def test_the_probe_query_the_console_runs_first_is_allowed_for_the_identity(clients: access.ConsoleClients) -> None:
    client = clients.client_for("svc-a")

    rows = client.execute(
        "SELECT count() AS n FROM system.one", settings={"additional_table_filters": "{'system.one': 'dummy = 1'}"}
    )

    assert rows == [{"n": 0}]
