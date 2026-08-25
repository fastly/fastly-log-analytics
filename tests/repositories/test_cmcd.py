"""Unit tests for backend.repositories.cmcd."""

from __future__ import annotations

from backend.core.log_fields import LOG_FIELD_CATALOG
from backend.provision.cmcd_fields import _CMCD_CUSTOM_FIELDS
from backend.repositories._base import _safe_table
from backend.repositories.cmcd import _response_cache, get_cmcd_aggregates
from tests.utils.mock_data import generate_mock_logs, insert_mock_logs


def _cmcd_logs(src, num=30):
    """Generate mock log entries with CMCD columns populated."""
    logs = generate_mock_logs(src, num_logs=num, hours_ago=1)
    for i, log in enumerate(logs):
        log["cmcd_sid"] = f"session_{i % 5}"
        log["cmcd_bl"] = 15 + i
        log["cmcd_br"] = 4500 + i * 100
        log["cmcd_bs"] = True if i % 4 == 0 else False
        log["cmcd_mtp"] = 6000 + i * 200
        log["cmcd_ot"] = "v"
        log["cmcd_sf"] = "h"
        log["cmcd_su"] = True if i % 10 == 0 else False
        log["cmcd_tb"] = 5000 + i * 100
        log["cmcd_rtp"] = 4800 + i * 100
        log["cmcd_cid"] = f"content_{i % 3}"
        log["cmcd_dl"] = 50 + i
        log["country"] = "US" if i % 2 == 0 else "GB"
        log["asn"] = 7922 if i % 2 == 0 else 3320
    return logs


def test_cmcd_aggregates_response_cache_hit(in_memory_duckdb, test_service_source):
    """Verify that get_cmcd_aggregates response caching works correctly on consecutive hits."""
    _response_cache.clear()

    src = test_service_source
    table_name = _safe_table(src["name"])

    # Pre-create the table with standard fields + CMCD fields
    raw_fields = [f for f in LOG_FIELD_CATALOG if f.get("vcl") is not None]
    schema_parts = [f'"{f["id"]}" {f["duckdb_type"]}' for f in raw_fields]
    for cf in _CMCD_CUSTOM_FIELDS:
        schema_parts.append(f'"{cf["name"]}" {cf["duckdb_type"]}')

    schema_def = ", ".join(schema_parts)
    in_memory_duckdb.execute(f"CREATE TABLE {table_name} ({schema_def})")

    # Generate and insert logs
    logs = _cmcd_logs(src, num=20)
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    # First call (Cold Miss)
    cold = get_cmcd_aggregates(
        con=in_memory_duckdb,
        src=src,
        start_time=None,
        end_time=None,
        filters={},
        bucket_seconds=300,
        top_n=5,
        sections={"overview", "sessions_ts", "top_content"},
        mask_ips=False,
    )
    assert cold["available"] is True
    assert cold.get("is_cached") is not True
    assert "overview" in cold

    # Second call (Warm Hit)
    warm = get_cmcd_aggregates(
        con=in_memory_duckdb,
        src=src,
        start_time=None,
        end_time=None,
        filters={},
        bucket_seconds=300,
        top_n=5,
        sections={"overview", "sessions_ts", "top_content"},
        mask_ips=False,
    )
    assert warm["available"] is True
    assert warm.get("is_cached") is True
    assert warm["overview"] == cold["overview"]
    assert warm["top_content"] == cold["top_content"]
