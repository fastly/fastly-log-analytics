"""Opt-in: the ClickHouse chart's separate provisioner account, rendered by ``helm`` and run on a REAL server.

RUN_HS_CONSOLE_ACCESS_INTEGRATION=1 uv run pytest tests/high_scale/test_console_provisioner_chart_integration.py

Renders ``deploy/chart/clickhouse`` with ``auth.accessManagement=false`` and ``provisioner.enabled=true``, boots a
disposable ``clickhouse-server`` from exactly that rendered configuration, and proves that

* the long-running application user can no longer create users or grant privileges,
* the provisioner's privilege set (as rendered by the chart) is sufficient to provision the analyst-console
  identities with the production code, and
* the provisioner holds nothing beyond access management and its grants on the application database.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import uuid
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest
import yaml

from backend.config import ClickHouseConfig
from backend.core.clickhouse_client import ClickHouseClient, ClickHouseError
from backend.high_scale import console_access as access
from tests.high_scale.conftest import (
    clickhouse_single_node_ddl as _ddl,
)
from tests.high_scale.conftest import (
    free_loopback_port as _free_port,
)
from tests.high_scale.conftest import (
    wait_for_clickhouse_ready,
)

pytestmark = [
    pytest.mark.security_regression,
    pytest.mark.skipif(
        os.getenv("RUN_HS_CONSOLE_ACCESS_INTEGRATION") != "1" or shutil.which("helm") is None,
        reason="explicit real ClickHouse server gate (and helm on PATH)",
    ),
]

IMAGE = os.getenv("RUM_CLICKHOUSE_TEST_IMAGE", "clickhouse/clickhouse-server:25.8.4.13")
DB = "fla_it"
CHART = "deploy/chart/clickhouse"
SECRET = b"chart-secret-" + b"z" * 32


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


def test_the_chart_renders_unchanged_by_default() -> None:
    docs = _render()

    assert _statefulset_env(docs)["CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT"] == "1"
    assert "CLICKHOUSE_PROVISIONER_PASSWORD" not in _statefulset_env(docs)
    config_map = next(d for d in docs if d["kind"] == "ConfigMap" and "-config" in d["metadata"]["name"])
    assert "provisioner-user.xml" not in config_map["data"]


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
def server() -> Iterator[dict]:
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
    xml = config_map["data"]["provisioner-user.xml"]
    workdir = Path(tempfile.mkdtemp(prefix="fla-prov-"))
    xml_path = workdir / "provisioner-user.xml"
    xml_path.write_text(xml)

    port = _free_port()
    name = f"fla-prov-it-{uuid.uuid4().hex[:8]}"
    # `docker cp` rather than a bind mount: the host path is not necessarily shared with the Docker VM, in which
    # case Docker silently mounts an empty directory in its place and the provisioner user never exists.
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

        def app_sql(sql: str) -> None:
            response = httpx.post(base, params={"database": DB}, content=sql, auth=("fla_user", "apppw"), timeout=120)
            assert response.status_code == 200, f"{sql[:60]} -> {response.text[:200]}"

        for statement in _ddl():
            app_sql(statement)  # the application user keeps full data-plane rights
        yield {"port": port, "base": base}
    finally:
        subprocess.run(["docker", "stop", name], capture_output=True)
        shutil.rmtree(workdir, ignore_errors=True)


def _client(server: dict, user: str, password: str) -> ClickHouseClient:
    return ClickHouseClient(
        ClickHouseConfig(host="127.0.0.1", port=server["port"], database=DB, user=user, password=password, secure=False)
    )


def test_the_application_user_can_no_longer_mint_users_or_grants(server: dict) -> None:
    app = _client(server, "fla_user", "apppw")
    try:
        assert app.execute("SELECT currentUser() AS u") == [{"u": "fla_user"}], "it must log in first"
        assert app.execute("SELECT count() AS n FROM request_facts") == [{"n": 0}], "data-plane access is intact"
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


def test_the_chart_rendered_provisioner_privileges_are_sufficient_to_provision(server: dict) -> None:
    prov = _client(server, "fla_provisioner", "provpw")
    try:
        users = access.provision_console_access(prov, DB, "svc-chart", SECRET)
    finally:
        prov.close()

    assert users == {
        "masked": access.console_identity("svc-chart", SECRET, True)[0],
        "raw": access.console_identity("svc-chart", SECRET, False)[0],
    }
    manager = access.ConsoleClients(
        settings=ClickHouseConfig(
            host="127.0.0.1", port=server["port"], database=DB, user="x", password="x", secure=False
        ),
        secret=SECRET,
    )
    try:
        for masked in (True, False):
            granted = manager.client_for("svc-chart", masked=masked)
            assert granted is not None
            view = granted.tables["request_facts"]
            assert granted.client.execute(f"SELECT count() AS n FROM {view}") == [{"n": 0}]
    finally:
        manager.close()


def test_the_provisioner_is_idempotent_and_the_app_user_still_cannot_read_what_it_created(server: dict) -> None:
    prov = _client(server, "fla_provisioner", "provpw")
    try:
        access.provision_console_access(prov, DB, "svc-chart", SECRET)
        access.provision_console_access(prov, DB, "svc-chart", SECRET)
    finally:
        prov.close()


def test_the_provisioner_has_no_privileges_outside_access_management_and_the_application_database(
    server: dict,
) -> None:
    prov = _client(server, "fla_provisioner", "provpw")
    try:
        assert prov.execute("SELECT currentUser() AS u") == [{"u": "fla_provisioner"}], "it must log in first"
        with pytest.raises(ClickHouseError):
            prov.execute("SELECT query FROM system.query_log LIMIT 1")
        with pytest.raises(ClickHouseError):
            prov.execute(f"INSERT INTO {DB}.request_facts (service_id) VALUES ('x')")
        with pytest.raises(ClickHouseError):
            prov.execute(f"CREATE TABLE {DB}.t (x UInt8) ENGINE = Memory")
        with pytest.raises(ClickHouseError):
            prov.execute("SELECT * FROM url('http://127.0.0.1:8123/?query=SELECT%201', 'CSV', 'x String')")
    finally:
        prov.close()
