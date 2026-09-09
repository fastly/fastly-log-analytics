"""The bounded prototype never relies on background merges for correctness."""

from unittest.mock import MagicMock

import pytest

from backend.core import clickhouse_schema as schema
from backend.core.clickhouse_client import CLICKHOUSE_FACT_COLUMNS
from backend.core.metadata.pg_schema import pg_schema_statements


def test_fact_schema_and_deduplication_key():
    ddl = schema.CLICKHOUSE_FACT_DDL
    assert "ReplacingMergeTree" in ddl
    assert "PARTITION BY (service_id, generation)" in ddl
    assert "ORDER BY (service_id, generation, batch_id, row_ordinal)" in ddl
    assert "DateTime64(6, 'UTC')" in ddl
    assert schema.clickhouse_fact_columns() == CLICKHOUSE_FACT_COLUMNS
    assert all(f"    {column} " in ddl for column in CLICKHOUSE_FACT_COLUMNS)
    assert "TTL" not in ddl


def test_postgres_manifest_is_additive_and_service_scoped():
    statements = pg_schema_statements()
    for name in ("datasets", "artifacts", "generations", "publications", "service_selection", "schema_state"):
        assert any(f"CREATE TABLE IF NOT EXISTS clickhouse_{name} (" in s for s in statements)
    assert "lease_fence BIGINT" in "\n".join(statements)
    assert "expires_at TIMESTAMPTZ NOT NULL" in "\n".join(statements)


def test_init_repeats_and_checks_actual_schema(monkeypatch):
    monkeypatch.setenv("METADATA_DSN", "postgresql://placeholder")
    client = MagicMock()
    monkeypatch.setattr(schema, "target_identity", lambda c: "target")
    record = MagicMock()
    monkeypatch.setattr(schema, "_record_schema", record)
    schema.create_clickhouse_schema(client)
    schema.create_clickhouse_schema(client)
    assert client.execute.call_count == 2
    assert record.call_count == 2


def test_reject_sqlite_before_ddl(monkeypatch):
    monkeypatch.delenv("METADATA_DSN", raising=False)
    client = MagicMock()
    with pytest.raises(RuntimeError, match="Postgres"):
        schema.create_clickhouse_schema(client)
    client.execute.assert_not_called()


def test_schema_validation_rejects_wrong_engine():
    client = MagicMock()
    client.execute.return_value = [{"uuid": "target", "engine": "MergeTree"}]
    with pytest.raises(RuntimeError, match="schema"):
        schema.target_identity(client)


def test_schema_validation_checks_uuid_columns_and_exact_keys():
    client = MagicMock()
    tables = [
        {
            "uuid": "target",
            "engine": "ReplacingMergeTree",
            "sorting_key": "service_id, generation, batch_id, row_ordinal",
            "partition_key": "(service_id, generation)",
        }
    ]
    columns = [{"name": name, "type": kind} for name, kind in schema.FACT_TYPES]
    client.execute.side_effect = [tables, columns]
    assert schema.target_identity(client) == "target"
    client.execute.side_effect = [tables, columns[:-1]]
    with pytest.raises(RuntimeError, match="columns mismatch"):
        schema.target_identity(client)


def test_schema_marker_uses_real_transaction_and_serializes_bootstrap(monkeypatch):
    monkeypatch.setenv("METADATA_DSN", "postgresql://placeholder")
    pool, con = MagicMock(), MagicMock()
    pool.connection.return_value.__enter__.return_value = con
    monkeypatch.setattr(schema.pg_connection, "get_pg_pool", lambda: pool)
    schema._record_schema("target")
    con.transaction.assert_called_once()
    assert "pg_advisory_xact_lock" in con.execute.call_args_list[0].args[0]
    assert con.execute.call_args.args[1] == ("target", schema.CLICKHOUSE_SCHEMA_VERSION)


def test_cli_disabled_and_cleanup(monkeypatch):
    monkeypatch.setattr(schema, "get_clickhouse_client", lambda: None)
    with pytest.raises(RuntimeError, match="disabled"):
        schema.main()
    client = MagicMock()
    pool = MagicMock()
    monkeypatch.setattr(schema, "get_clickhouse_client", lambda: client)
    monkeypatch.setattr(schema, "create_clickhouse_schema", lambda c: None)
    monkeypatch.setattr(schema.pg_connection, "is_postgres", lambda: True)
    monkeypatch.setattr(schema.pg_connection, "get_pg_pool", lambda: pool)
    schema.main()
    client.close.assert_called_once()
    pool.close.assert_called_once()
