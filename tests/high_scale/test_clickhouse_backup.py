import pytest

from backend.high_scale.clickhouse_backup import run_incremental_clickhouse_backup


class _FakeClient:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows
        self.last_sql: str | None = None

    def execute(self, sql: str, params: dict | None = None) -> list[dict]:
        self.last_sql = sql
        return self.rows


def test_backup_reports_completed_status_and_sizes() -> None:
    client = _FakeClient(
        [{"id": "backup-1", "status": "BACKUP_CREATED", "uncompressed_size": 1000, "compressed_size": 200, "error": ""}]
    )

    receipt = run_incremental_clickhouse_backup(client, table="request_facts", destination="fos_backup")

    assert receipt.backup_id == "backup-1"
    assert receipt.status == "completed"
    assert receipt.uncompressed_size == 1000
    assert receipt.compressed_size == 200
    assert receipt.error is None
    assert "BACKUP TABLE `request_facts`" in client.last_sql
    assert "fos_backup" in client.last_sql


def test_backup_reports_failed_status_with_error() -> None:
    client = _FakeClient([{"id": "backup-2", "status": "BACKUP_FAILED", "error": "disk unavailable"}])

    receipt = run_incremental_clickhouse_backup(client, table="request_facts", destination="fos_backup")

    assert receipt.status == "failed"
    assert receipt.error == "disk unavailable"


def test_backup_rejects_a_table_outside_the_high_scale_allowlist() -> None:
    client = _FakeClient([])

    with pytest.raises(ValueError, match="not internally allowlisted"):
        run_incremental_clickhouse_backup(client, table="drop_all_data", destination="fos_backup")


def test_backup_rejects_an_unsafe_destination_identifier() -> None:
    client = _FakeClient([])

    with pytest.raises(ValueError, match="destination"):
        run_incremental_clickhouse_backup(client, table="request_facts", destination="fos_backup; DROP TABLE x")
