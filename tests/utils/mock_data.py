import json
import random
from datetime import UTC, datetime, timedelta

from backend.core.log_fields import LOG_FIELD_CATALOG, resolve_enabled_fields


def generate_mock_logs(config: dict, num_logs: int = 100, hours_ago: int = 1) -> list[dict]:
    """
    Generate synthetic log entries based on the log_fields schema.
    """
    enabled_fields = resolve_enabled_fields(config.get("log_fields"))
    logs = []

    now = datetime.now(UTC)
    start_time = now - timedelta(hours=hours_ago)

    # Common IPs and User Agents for realistic aggregations
    ips = ["192.168.1.1", "10.0.0.1", "203.0.113.5", "198.51.100.10"]
    uas = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/605.1.15",
        "curl/7.68.0",
        "python-requests/2.25.1",
    ]
    urls = ["/", "/api/data", "/images/logo.png", "/about", "/login"]

    for _ in range(num_logs):
        log_entry = {}

        # Advance time slightly for each log to spread them out
        log_time = start_time + timedelta(seconds=random.randint(0, hours_ago * 3600))

        # Always-on fields
        if "timestamp" in enabled_fields:
            log_entry["timestamp"] = log_time.strftime("%Y-%m-%dT%H:%M:%S%z")
            # DuckDB expects a valid format, python's %z leaves off the colon, let's fix it
            if log_entry["timestamp"].endswith("0000"):
                log_entry["timestamp"] = log_entry["timestamp"][:-4] + "00:00"

        if "ip" in enabled_fields:
            log_entry["ip"] = random.choice(ips)

        if "status" in enabled_fields:
            # 80% 200s, 10% 404s, 10% 500s
            log_entry["status"] = random.choices([200, 404, 500], weights=[0.8, 0.1, 0.1])[0]

        if "elapsed" in enabled_fields:
            log_entry["elapsed"] = random.randint(1000, 500000)  # 1ms to 500ms

        if "cache" in enabled_fields:
            log_entry["cache"] = random.choices(["HIT", "MISS", "PASS"], weights=[0.6, 0.3, 0.1])[0]

        if "resp_bytes" in enabled_fields:
            log_entry["resp_bytes"] = random.randint(500, 50000)

        # Group A
        if "url" in enabled_fields:
            log_entry["url"] = random.choice(urls)
        if "ua" in enabled_fields:
            log_entry["ua"] = random.choice(uas)
        if "method" in enabled_fields:
            log_entry["method"] = "GET"

        # Group C (Infrastructure)
        if "pop" in enabled_fields:
            log_entry["pop"] = random.choice(["JFK", "LHR", "SYD"])
        if "ttfb" in enabled_fields:
            # TTFB usually smaller than elapsed. Convert back to seconds for the log format
            ttfb_ms = log_entry.get("elapsed", 20000) * 0.8
            log_entry["ttfb"] = f"{ttfb_ms / 1000000:.6f}"

        # Group D (Geo)
        if "country" in enabled_fields:
            log_entry["country"] = random.choice(["US", "GB", "AU", "DE"])

        # Group F (Network)
        if "asn" in enabled_fields:
            log_entry["asn"] = random.choice([7922, 3320, 15169])

        logs.append(log_entry)

    return logs


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
        schema_def = ", ".join([f'"{f["id"]}" {f["duckdb_type"]}' for f in raw_fields])

        # Create table if it doesn't exist
        con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({schema_def})")

        # Load the JSON data
        con.execute(f"INSERT INTO {table_name} BY NAME SELECT * FROM read_json_auto('{temp_path}')")
    finally:
        os.unlink(temp_path)
