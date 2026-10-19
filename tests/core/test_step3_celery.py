from unittest.mock import patch

from backend.core.ingest import convert
from backend.core.metadata.base import get_con


def test_celery_ledger_claim_semantics(monkeypatch):
    service_id = "test-celery-svc"
    object_key = "raw/2026/08/27/10/05/logs.json.gz"

    con = get_con(service_id)
    cur = con.cursor()
    cur.execute("DELETE FROM ingest_ledger WHERE service_id=?", (service_id,))
    cur.execute(
        "INSERT INTO ingest_ledger (service_id, object_key, status) VALUES (?, ?, 'discovered')",
        (service_id, object_key),
    )
    con.commit()

    class MockRequest:
        def __init__(self, wid):
            self.id = wid

    class MockTask:
        def __init__(self, wid):
            self.request = MockRequest(wid)

    cur.execute(
        "UPDATE ingest_ledger SET status='claimed', claimed_by='worker-1' WHERE service_id=? AND object_key=?",
        (service_id, object_key),
    )
    con.commit()

    with patch("backend.core.duckdb.get_source_for_service", return_value={"name": "test"}):
        with patch("backend.core.duckdb.get_connection", return_value=None):
            with patch("backend.core.iceberg._ducklake_attach", return_value=None):
                with patch("backend.core.iceberg._ducklake_add_data_files", return_value=None):
                    convert.push_request(id="worker-2")
                    convert(service_id, object_key)

    cur.execute(
        "SELECT status, claimed_by FROM ingest_ledger WHERE service_id=? AND object_key=?", (service_id, object_key)
    )
    row = cur.fetchone()
    assert row["status"] == "claimed"
    assert row["claimed_by"] == "worker-1"
