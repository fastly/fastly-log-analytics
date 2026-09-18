import re
from pathlib import Path
from backend.core.clickhouse_client import get_clickhouse_client
import logging

_SCHEMA_FILES = [
    "publication_schema.sql",
    "archive_manifest_schema.sql",
    "request_schema.sql",
    "rum_schema.sql",
    "cmcd_schema.sql",
    "rum_aggregate_schema.sql",
    "cmcd_aggregate_schema.sql",
    "origin_schema.sql",
    "network_schema.sql",
    "security_schema.sql",
    "performance_schema.sql",
]

def install_clickhouse_schema() -> None:
    """Install the idempotent ClickHouse schema required by high-scale workers."""
    client = get_clickhouse_client()
    if not client:
        return
        
    sql_root = Path(__file__).with_name("sql")
    
    for filename in _SCHEMA_FILES:
        path = sql_root / filename
        if not path.exists():
            continue
            
        content = path.read_text("utf-8")
        
        # Replace Replicated engines with local prototype ones
        content = re.sub(r"ReplicatedReplacingMergeTree\((.*?)\)\s*\('/clickhouse/[^']+',\s*'[^']+'\)", r"ReplacingMergeTree(\1)", content)
        content = re.sub(r"ReplicatedMergeTree\('/clickhouse/[^']+',\s*'[^']+'\)", "MergeTree()", content)
        content = re.sub(r"ReplicatedReplacingMergeTree\(([^)]+)\)\n\s*\('[^']+',\s*'[^']+'\)", r"ReplacingMergeTree(\1)", content)
        
        for stmt in content.split(";"):
            stmt = stmt.strip()
            if stmt and not stmt.startswith("--"):
                try:
                    client.execute(stmt)
                except Exception as e:
                    logging.getLogger(__name__).warning("Failed to install schema %s: %s", filename, e)

if __name__ == "__main__":
    install_clickhouse_schema()
