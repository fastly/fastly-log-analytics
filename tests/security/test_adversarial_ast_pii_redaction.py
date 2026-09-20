"""Adversarial AST query rewriting tests for PII and IP masking.

Validates that when an analyst session has PII masking enabled (mask_ips=True),
the backend's source-level AST query rewriting (SELECT * REPLACE (...))
guarantees that raw client IPs and edge session cookies cannot be exfiltrated
through any SQL projection, alias, string operation, type cast, CTE, subquery,
aggregation, or conditional expression.
"""

from __future__ import annotations

import duckdb
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.core import share_db
from backend.repositories.query import execute_query
from backend.routers import share_admin, share_auth
from backend.utils import tunnel
from backend.utils.remote_access import RemoteAccessMiddleware

RAW_IP = "198.51.100.42"
RAW_SESSION = "deadbeefcafe1234567890abcdef"
RAW_URL = "/api/v1/sensitive-resource"
SOURCE_NAME = "default"
TIME_FILTER = ("2026-09-19T00:00:00Z", "2026-09-19T23:59:59Z")


@pytest.fixture
def test_db() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    con.execute(
        """
        CREATE TABLE logs (
            timestamp TIMESTAMPTZ,
            ip VARCHAR,
            client_ip VARCHAR,
            cookie_session VARCHAR,
            status INTEGER,
            url VARCHAR
        );
        INSERT INTO logs VALUES (
            '2026-09-19 12:00:00Z'::TIMESTAMPTZ,
            '198.51.100.42',
            '198.51.100.42',
            'deadbeefcafe1234567890abcdef',
            200,
            '/api/v1/sensitive-resource'
        );
        """
    )
    yield con
    con.close()


def test_admin_and_pii_analyst_sees_unmasked_pii(test_db: duckdb.DuckDBPyConnection) -> None:
    """Admin and Analyst-with-PII (mask_ips=False) see raw IPs and session hashes."""
    sql = "SELECT ip, client_ip, cookie_session, url FROM logs"
    res = execute_query(
        test_db,
        {"name": SOURCE_NAME},
        sql,
        max_rows=10,
        want_explain=False,
        mask_ips=False,
        time_filter=TIME_FILTER,
    )
    rows = res["data"]
    assert len(rows) == 1
    assert rows[0]["ip"] == RAW_IP
    assert rows[0]["client_ip"] == RAW_IP
    assert rows[0]["cookie_session"] == RAW_SESSION
    assert rows[0]["url"] == RAW_URL


def test_no_pii_analyst_direct_projection_redacted(test_db: duckdb.DuckDBPyConnection) -> None:
    """Analyst without PII (mask_ips=True) receives [redacted] on all PII columns."""
    sql = "SELECT ip, client_ip, cookie_session, url FROM logs"
    res = execute_query(
        test_db,
        {"name": SOURCE_NAME},
        sql,
        max_rows=10,
        want_explain=False,
        mask_ips=True,
        time_filter=TIME_FILTER,
    )
    rows = res["data"]
    assert len(rows) == 1
    assert rows[0]["ip"] == "[redacted]"
    assert rows[0]["client_ip"] == "[redacted]"
    assert rows[0]["cookie_session"] == "[redacted]"
    # Non-PII fields remain verbatim
    assert rows[0]["url"] == RAW_URL


def test_adversarial_ast_string_concatenation(test_db: duckdb.DuckDBPyConnection) -> None:
    """Attacker attempts string concatenation to defeat key-based or regex post-masking.

    E.g. SELECT 'prefix_' || ip || '_suffix' AS exfil FROM logs
    The AST source view replacement ensures 'ip' is already '[redacted]' prior to concat.
    """
    sql = "SELECT 'prefix_' || ip || '_suffix' AS exfil FROM logs"
    res = execute_query(
        test_db,
        {"name": SOURCE_NAME},
        sql,
        max_rows=10,
        want_explain=False,
        mask_ips=True,
        time_filter=TIME_FILTER,
    )
    rows = res["data"]
    assert len(rows) == 1
    assert rows[0]["exfil"] == "prefix_[redacted]_suffix"
    assert RAW_IP not in rows[0]["exfil"]


def test_adversarial_ast_type_cast(test_db: duckdb.DuckDBPyConnection) -> None:
    """Attacker attempts type casting (CAST(ip AS VARCHAR) or ip::TEXT)."""
    sql = "SELECT CAST(ip AS VARCHAR) AS cast_ip, cookie_session::TEXT AS cast_sess FROM logs"
    res = execute_query(
        test_db,
        {"name": SOURCE_NAME},
        sql,
        max_rows=10,
        want_explain=False,
        mask_ips=True,
        time_filter=TIME_FILTER,
    )
    rows = res["data"]
    assert len(rows) == 1
    assert rows[0]["cast_ip"] == "[redacted]"
    assert rows[0]["cast_sess"] == "[redacted]"


def test_adversarial_ast_substring_slicing(test_db: duckdb.DuckDBPyConnection) -> None:
    """Attacker attempts character-by-character extraction via SUBSTR()."""
    sql = "SELECT SUBSTR(ip, 1, 4) AS sub_ip FROM logs"
    res = execute_query(
        test_db,
        {"name": SOURCE_NAME},
        sql,
        max_rows=10,
        want_explain=False,
        mask_ips=True,
        time_filter=TIME_FILTER,
    )
    rows = res["data"]
    assert len(rows) == 1
    assert rows[0]["sub_ip"] == "[red"
    assert RAW_IP[:4] not in rows[0]["sub_ip"]


def test_adversarial_ast_cte_wrapper(test_db: duckdb.DuckDBPyConnection) -> None:
    """Attacker attempts aliasing and projection within a Common Table Expression (CTE)."""
    sql = """
    WITH attacker_cte AS (
        SELECT ip AS aliased_ip, cookie_session AS aliased_sess FROM logs
    )
    SELECT aliased_ip, aliased_sess FROM attacker_cte
    """
    res = execute_query(
        test_db,
        {"name": SOURCE_NAME},
        sql,
        max_rows=10,
        want_explain=False,
        mask_ips=True,
        time_filter=TIME_FILTER,
    )
    rows = res["data"]
    assert len(rows) == 1
    assert rows[0]["aliased_ip"] == "[redacted]"
    assert rows[0]["aliased_sess"] == "[redacted]"


def test_adversarial_ast_subquery_wrapper(test_db: duckdb.DuckDBPyConnection) -> None:
    """Attacker attempts nested subquery extraction."""
    sql = "SELECT sub.x, sub.y FROM (SELECT ip AS x, cookie_session AS y FROM logs) AS sub"
    res = execute_query(
        test_db,
        {"name": SOURCE_NAME},
        sql,
        max_rows=10,
        want_explain=False,
        mask_ips=True,
        time_filter=TIME_FILTER,
    )
    rows = res["data"]
    assert len(rows) == 1
    assert rows[0]["x"] == "[redacted]"
    assert rows[0]["y"] == "[redacted]"


def test_adversarial_ast_conditional_oracle(test_db: duckdb.DuckDBPyConnection) -> None:
    """Attacker attempts boolean oracle / side-channel via CASE WHEN."""
    sql = f"""
    SELECT CASE
        WHEN ip = '{RAW_IP}' THEN 'leaked_raw_ip'
        ELSE 'protected_redacted'
    END AS oracle_probe FROM logs
    """
    res = execute_query(
        test_db,
        {"name": SOURCE_NAME},
        sql,
        max_rows=10,
        want_explain=False,
        mask_ips=True,
        time_filter=TIME_FILTER,
    )
    rows = res["data"]
    assert len(rows) == 1
    # Since ip was rebound to '[redacted]', equality against RAW_IP is False
    assert rows[0]["oracle_probe"] == "protected_redacted"


def test_adversarial_ast_where_filter(test_db: duckdb.DuckDBPyConnection) -> None:
    """Attacker attempts to locate rows matching a specific raw IP."""
    sql = f"SELECT url FROM logs WHERE ip = '{RAW_IP}'"
    res = execute_query(
        test_db,
        {"name": SOURCE_NAME},
        sql,
        max_rows=10,
        want_explain=False,
        mask_ips=True,
        time_filter=TIME_FILTER,
    )
    rows = res["data"]
    # Replaced column does not match raw IP; returns 0 rows
    assert len(rows) == 0


def test_adversarial_ast_group_by_aggregation(test_db: duckdb.DuckDBPyConnection) -> None:
    """Attacker attempts grouping by IP to count distinct IP occurrences."""
    sql = "SELECT ip, COUNT(*) AS count_rows FROM logs GROUP BY ip"
    res = execute_query(
        test_db,
        {"name": SOURCE_NAME},
        sql,
        max_rows=10,
        want_explain=False,
        mask_ips=True,
        time_filter=TIME_FILTER,
    )
    rows = res["data"]
    assert len(rows) == 1
    assert rows[0]["ip"] == "[redacted]"
    assert rows[0]["count_rows"] == 1


# ── Analyst RBAC & PII Policy Middleware Tests ──────────────────────────────


@pytest.fixture
def rbac_app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(RemoteAccessMiddleware)
    app.include_router(share_auth.router)
    app.include_router(share_admin.router)

    @app.get("/api/dashboard")
    def _dash():
        return {"ok": True}

    @app.post("/api/views")
    def _create_view():
        return {"ok": True}

    @app.post("/api/dashboard/aggregates")
    def _aggregates():
        return {"ok": True}

    return app


def test_analyst_rbac_admin_routes_blocked(rbac_app: FastAPI) -> None:
    """Analyst sessions are blocked from administrative endpoints (403 admin_only)."""
    tunnel.get_tunnel_manager().start_sharing(public_endpoint="https://testserver")
    invite = share_db.create_remote_invite(
        name="RBAC Admin Test",
        email="rbac-admin-test@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=["svcA"],
        pii_policy={"mask_ips": True},
    )
    tos = share_db.get_latest_tos()
    if tos:
        share_db.mark_tos_accepted(invite["id"], tos["version"])

    client = TestClient(rbac_app)
    r_login = client.post(
        "/api/share/login",
        json={"email": "rbac-admin-test@example.com", "passcode": "ocean-breeze-cabin-42"},
        headers={"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"},
    )
    assert r_login.status_code == 200

    for raw in r_login.headers.get_list("set-cookie"):
        name, _, val = raw.split(";", 1)[0].partition("=")
        val = val.strip('"')
        if name == "analyst_session_id" and val:
            client.cookies.set("analyst_session_id", val)

    r_admin = client.get(
        "/api/admin/share/status",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r_admin.status_code == 403
    assert r_admin.json()["error"] == "admin_only"


def test_analyst_rbac_mutation_routes_blocked(rbac_app: FastAPI) -> None:
    """Analyst sessions are blocked from write/mutation endpoints (403 read_only)."""
    tunnel.get_tunnel_manager().start_sharing(public_endpoint="https://testserver")
    invite = share_db.create_remote_invite(
        name="RBAC Write Test",
        email="rbac-write-test@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=["svcA"],
        pii_policy={"mask_ips": True},
    )
    tos = share_db.get_latest_tos()
    if tos:
        share_db.mark_tos_accepted(invite["id"], tos["version"])

    client = TestClient(rbac_app)
    r_login = client.post(
        "/api/share/login",
        json={"email": "rbac-write-test@example.com", "passcode": "ocean-breeze-cabin-42"},
        headers={"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"},
    )
    assert r_login.status_code == 200

    for raw in r_login.headers.get_list("set-cookie"):
        name, _, val = raw.split(";", 1)[0].partition("=")
        val = val.strip('"')
        if name == "analyst_session_id" and val:
            client.cookies.set("analyst_session_id", val)

    r_mut = client.post(
        "/api/views",
        headers={"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"},
    )
    assert r_mut.status_code == 403
    assert r_mut.json()["error"] == "read_only"


def test_analyst_pii_policy_filter_locking(rbac_app: FastAPI) -> None:
    """Analyst with mask_ips=True is blocked from filtering on PII fields (403 pii_policy_violation),

    while allowed reads without PII filtering succeed with 200 OK.
    """
    tunnel.get_tunnel_manager().start_sharing(public_endpoint="https://testserver")
    invite = share_db.create_remote_invite(
        name="RBAC Filter Lock Test",
        email="rbac-filter-lock@example.com",
        passcode="ocean-breeze-cabin-42",
        expires_at_utc=None,
        ip_whitelist=None,
        service_ids=["svcA"],
        pii_policy={"mask_ips": True},
    )
    tos = share_db.get_latest_tos()
    if tos:
        share_db.mark_tos_accepted(invite["id"], tos["version"])

    client = TestClient(rbac_app)
    r_login = client.post(
        "/api/share/login",
        json={"email": "rbac-filter-lock@example.com", "passcode": "ocean-breeze-cabin-42"},
        headers={"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"},
    )
    assert r_login.status_code == 200

    for raw in r_login.headers.get_list("set-cookie"):
        name, _, val = raw.split(";", 1)[0].partition("=")
        val = val.strip('"')
        if name == "analyst_session_id" and val:
            client.cookies.set("analyst_session_id", val)

    # 1. PII filter attempt is blocked
    r_filt = client.post(
        "/api/dashboard/aggregates?service=svcA",
        json={"filters": {"ip": "198.51.100.42"}},
        headers={"X-Remote-Analyst": "1", "Host": "testserver", "Origin": "https://testserver"},
    )
    assert r_filt.status_code == 403
    assert r_filt.json()["error"] == "pii_policy_violation"
    assert r_filt.json()["field"] == "ip"

    # 2. Allowed read query without PII filters succeeds
    r_ok = client.get(
        "/api/dashboard?service=svcA",
        headers={"X-Remote-Analyst": "1", "Host": "testserver"},
    )
    assert r_ok.status_code == 200
    assert r_ok.json()["ok"] is True
