"""Fastly Value list rows keep the repository's wire bytes through their row models."""

from __future__ import annotations

from backend.models.value import IOFormatConversionPair, IOOpportunityRow, TopBotRow, WafSignalRow
from backend.repositories._base import _safe_table
from backend.repositories.value import get_summary
from tests._helpers.wire import assert_rows_wire_identical
from tests.utils.mock_data import generate_mock_logs, insert_mock_logs


def _summary(con, src):
    logs = generate_mock_logs(src, num_logs=120, hours_ago=1)
    for i, log in enumerate(logs):
        log["waf_sig"] = ["SQLI,XSS", "XSS", "VERIFIED-BOT.search-engine"][i % 3]
        log["_bot_name"] = ["googlebot", "bingbot", ""][i % 3]
        log["url"] = f"/img/{i % 4}.jpg"
        log["io_input_bytes"] = 100_000 + i
        log["io_output_bytes"] = 40_000 + i
        log["io_input_format"] = ["jpeg", "png"][i % 2]
        log["io_output_format"] = ["webp", "avif"][i % 2]
        log["resp_bytes"] = 50_000 + i
    insert_mock_logs(con, _safe_table(src["name"]), logs)
    return get_summary(
        con,
        src,
        None,
        None,
        {},
        sections={"security", "bots", "io"},
        service_id=None,
    )


def test_value_list_rows_wire_identical(in_memory_duckdb, test_service_source):
    res = _summary(in_memory_duckdb, test_service_source)
    assert_rows_wire_identical(WafSignalRow, res["security"]["top_waf_signals"])
    assert_rows_wire_identical(TopBotRow, res["bots"]["top_bots"])
    assert_rows_wire_identical(IOOpportunityRow, res["io"]["optimization_opportunities"])
    assert_rows_wire_identical(IOFormatConversionPair, res["io"]["io_format_conversion_pairs"])
