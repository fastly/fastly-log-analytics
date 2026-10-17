"""Analyst-visible error text must carry neither the cid salt nor absolute server paths."""

from __future__ import annotations

from unittest.mock import patch

import duckdb
import pytest

from backend.core.query_registry import _scrub_secrets
from backend.repositories import query as q
from backend.utils.error_redaction import redact_error_text, register_salt
from tests.conftest import MOCK_SERVICE_ID

pytestmark = pytest.mark.security_regression

SALT = "0123456789abcdefSALTVALUE-zz"
SVC = "svc"
WINDOW = ("2020-01-01T00:00:00Z", "2099-01-01T00:00:00Z")


@pytest.fixture(autouse=True)
def _known_salt():
    register_salt(SALT)


@pytest.mark.parametrize(
    "text",
    [
        f"Binder Error near sha256(CONCAT('{SALT}', cid))",
        f'Binder Error near "sha256"("CONCAT"(\'{SALT}\', cid))',
        f"Conversion Error: '{SALT}' || cid",
        f"concat_ws('', '{SALT}', cid) failed",
        f"first {SALT} and again {SALT} and sha256(CONCAT('{SALT}', x))",
    ],
)
def test_salt_is_redacted_by_value_in_every_embedding(text):
    assert SALT not in redact_error_text(text)
    assert SALT not in _scrub_secrets(text)


def test_unknown_literal_form_still_caught_by_regex_fallback():
    out = redact_error_text("x sha256(CONCAT('someOtherSalt123', cid))")
    assert "someOtherSalt123" not in out


def test_paths_and_salt_both_removed():
    out = redact_error_text(f"IO Error: '/app/data/services/x/y.duckdb' salt={SALT}")
    assert "/app/data" not in out and SALT not in out


@pytest.mark.parametrize(
    "analyst_sql",
    [
        "SELECT CAST(cid AS INTEGER) FROM logs",
        "SELECT nonexistent_col FROM logs",
        "SELECT cid::DATE FROM logs",
        "SELECT * FROM logs WHERE cid = (SELECT 1/0)",
        "SELECT read_text('/etc/passwd')",
    ],
)
def test_crafted_analyst_query_errors_leak_neither_salt_nor_paths(analyst_sql):
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE logs_svc (timestamp TIMESTAMPTZ, cid VARCHAR, ip VARCHAR)")
    con.execute("INSERT INTO logs_svc VALUES (now(), 'client-a', '1.2.3.4')")
    with patch("backend.core.duckdb.get_or_generate_cid_salt", return_value=SALT):
        with pytest.raises(Exception) as exc:
            q.execute_query(
                con,
                {"name": SVC, "service_id": SVC},
                analyst_sql,
                100,
                False,
                service_id=SVC,
                mask_ips=True,
                time_filter=WINDOW,
            )
    con.close()
    shown = redact_error_text(exc.value)
    assert SALT not in str(exc.value)
    assert SALT not in shown
    assert "/etc/" not in shown and "/app/" not in shown


@pytest.mark.parametrize("exc_cls", [PermissionError, RuntimeError])
def test_query_route_error_bodies_are_scrubbed(client, exc_cls):
    msg = f"boom '/srv/data/x.parquet' sha256(CONCAT('{SALT}', cid)) {SALT}"
    with patch("backend.repositories.query.execute_query", side_effect=exc_cls(msg)):
        resp = client.post(
            "/api/query",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"sql": "SELECT 1"},
        )
    assert resp.status_code in (400, 403)
    assert SALT not in resp.text
    assert "/srv/" not in resp.text
