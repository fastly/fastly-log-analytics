import json
import sys
from pathlib import Path

from backend.core.log_fields import LOG_FIELD_CATALOG

# The canonical mock-record schema + distributions live in scripts/lib/log_factory.py
# so the bulk synthetic-traffic generator and these test fixtures share one source of
# truth. See AGENTS.md (synthetic traffic generation).
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from scripts.lib.log_factory import generate_mock_records


def generate_mock_logs(config: dict, num_logs: int = 100, hours_ago: int = 1) -> list[dict]:
    """
    Generate synthetic log entries based on the log_fields schema.

    Delegates to ``scripts.lib.log_factory.generate_mock_records`` (stdlib ``random``
    default) so output stays byte-identical to the historical implementation.
    """
    return generate_mock_records(config, num_logs=num_logs, hours_ago=hours_ago)


def insert_mock_logs(con, table_name: str, logs: list[dict]):
    """
    Inserts a list of mock JSON logs directly into the DuckDB connection.
    This bypasses the S3 download step for testing repositories.
    """
    import os
    import tempfile

    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
        for log in logs:
            f.write(json.dumps(log) + "\n")
        temp_path = f.name

    try:
        # We need to map JSON fields to DuckDB schema types, similar to ingest.py
        # Filter to only fields that have VCL (raw log fields), excluding computed metrics
        raw_fields = [f for f in LOG_FIELD_CATALOG if f.get("vcl") is not None]
        columns_map = {f["id"]: ("TIMESTAMPTZ" if f["id"] == "timestamp" else f["duckdb_type"]) for f in raw_fields}
        for log in logs:
            for k, v in log.items():
                if k not in columns_map:
                    if isinstance(v, bool):
                        columns_map[k] = "BOOLEAN"
                    elif isinstance(v, int):
                        columns_map[k] = "BIGINT"
                    elif isinstance(v, float):
                        columns_map[k] = "DOUBLE"
                    else:
                        columns_map[k] = "VARCHAR"

        schema_def = ", ".join([f'"{col}" {dtype}' for col, dtype in columns_map.items()])

        # Create table if it doesn't exist
        con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({schema_def})")
        for col, dtype in columns_map.items():
            try:
                con.execute(f'ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS "{col}" {dtype}')
            except Exception:
                pass

        cols_sql = "{" + ", ".join(f"'{k}': '{v}'" for k, v in columns_map.items()) + "}"

        # Load the JSON data
        con.execute(f"INSERT INTO {table_name} BY NAME SELECT * FROM read_json_auto('{temp_path}', columns={cols_sql})")
    finally:
        os.unlink(temp_path)
