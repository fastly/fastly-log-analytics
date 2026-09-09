"""Versioned bounded serving index. Invoke explicitly with ``python -m ...``.

One finite dataset per service/generation is the partition, not a general
retention design. The sorting key preserves identical source rows by ordinal.
Every consumer MUST read FINAL, including verification before activation.
"""

from __future__ import annotations

import logging

from backend.core.clickhouse_client import CLICKHOUSE_FACT_COLUMNS, ClickHouseClient, get_clickhouse_client
from backend.core.metadata import pg_connection
from backend.core.metadata.clickhouse_ddl import CLICKHOUSE_CONTROL_DDL

CLICKHOUSE_SCHEMA_VERSION = 1
FACT_TYPES = (
    ("service_id", "String"),
    ("batch_id", "String"),
    ("source_identity", "String"),
    ("row_ordinal", "UInt64"),
    ("generation", "String"),
    ("timestamp", "DateTime64(6, 'UTC')"),
    ("country", "Nullable(String)"),
    ("ip", "Nullable(String)"),
    ("url", "Nullable(String)"),
    ("conn_requests", "Nullable(Int64)"),
)
CLICKHOUSE_FACT_DDL = """
CREATE TABLE IF NOT EXISTS log_facts (
    service_id String,
    batch_id String,
    source_identity String,
    row_ordinal UInt64,
    generation String,
    timestamp DateTime64(6, 'UTC'),
    country Nullable(String),
    ip Nullable(String),
    url Nullable(String),
    conn_requests Nullable(Int64)
) ENGINE = ReplacingMergeTree
PARTITION BY (service_id, generation)
ORDER BY (service_id, generation, batch_id, row_ordinal)
"""


def clickhouse_fact_columns() -> tuple[str, ...]:
    return CLICKHOUSE_FACT_COLUMNS


def require_postgres() -> None:
    if not pg_connection.is_postgres():
        raise RuntimeError("ClickHouse prototype requires Postgres METADATA_DSN; SQLite is unsupported")


def target_identity(client: ClickHouseClient) -> str:
    """Proof of the actual table, not a host name (a replacement volume differs)."""
    tables = client.execute(
        "SELECT toString(uuid) AS uuid, engine, sorting_key, partition_key FROM system.tables "
        "WHERE database = currentDatabase() AND name = 'log_facts'"
    )
    if (
        len(tables) != 1
        or tables[0].get("engine") != "ReplacingMergeTree"
        or tables[0].get("sorting_key") != "service_id, generation, batch_id, row_ordinal"
        or tables[0].get("partition_key") != "(service_id, generation)"
        or tables[0].get("uuid") in (None, "", "00000000-0000-0000-0000-000000000000")
    ):
        raise RuntimeError("ClickHouse fact schema mismatch or missing table identity")
    columns = client.execute(
        "SELECT name, type FROM system.columns "
        "WHERE database = currentDatabase() AND table = 'log_facts' ORDER BY position"
    )
    if [(r["name"], r["type"]) for r in columns] != list(FACT_TYPES):
        raise RuntimeError("ClickHouse fact schema columns mismatch")
    return tables[0]["uuid"]


def _record_schema(identity: str) -> None:
    require_postgres()
    # The SQLite-shaped wrapper's commit() is a no-op. Use a real psycopg
    # transaction even though the underlying pool connections are autocommit.
    with pg_connection.get_pg_pool().connection() as con, con.transaction():
        con.execute("SELECT pg_advisory_xact_lock(2080406)")
        for ddl in CLICKHOUSE_CONTROL_DDL:
            con.execute(ddl)
        con.execute(
            "INSERT INTO clickhouse_schema_state(target_identity, schema_version) VALUES (%s, %s) "
            "ON CONFLICT (target_identity) DO UPDATE SET "
            "schema_version = EXCLUDED.schema_version, checked_at = clock_timestamp()",
            (identity, CLICKHOUSE_SCHEMA_VERSION),
        )


def create_clickhouse_schema(client: ClickHouseClient) -> None:
    require_postgres()
    client.execute(CLICKHOUSE_FACT_DDL)
    _record_schema(target_identity(client))


def main() -> None:
    client = get_clickhouse_client()
    if client is None:
        raise RuntimeError("ClickHouse prototype is disabled")
    try:
        create_clickhouse_schema(client)
        logging.getLogger(__name__).info("ClickHouse schema v%s verified", CLICKHOUSE_SCHEMA_VERSION)
    finally:
        client.close()
        if pg_connection.is_postgres():
            pg_connection.get_pg_pool().close()


if __name__ == "__main__":
    main()
