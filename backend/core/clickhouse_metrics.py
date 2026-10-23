"""Lazy low-cardinality ClickHouse instruments on the existing OTel provider."""

import logging
from functools import cache

from backend.core.request_telemetry import get_meter

logger = logging.getLogger(__name__)


@cache
def _instrument(name: str, kind: str, unit: str = ""):
    meter = get_meter()
    create = meter.create_histogram if kind == "histogram" else meter.create_counter
    return create(f"app.clickhouse_{name}", unit=unit)


def record_operation(stats: dict) -> None:
    if stats["operation"] not in {"execute", "insert"}:
        return
    try:
        insert = stats["operation"] == "insert"
        _instrument("insert_duration_ms" if insert else "query_duration_ms", "histogram", "ms").record(
            stats["duration_ms"], {"outcome": stats["outcome"]}
        )
        for phase in ("admission_wait_ms", "network_ms", "decode_ms", "server_elapsed_ms"):
            value = stats.get(phase)
            if value is not None:
                _instrument(phase, "histogram", "ms").record(
                    value, {"operation": stats["operation"], "outcome": stats["outcome"]}
                )
        if not insert:
            _instrument("queries_total", "counter").add(1, {"outcome": stats["outcome"]})
        elif stats["outcome"] == "success":
            # Physical acknowledged rows, including identical retry copies.
            _instrument("rows_inserted_total", "counter").add(stats["rows_written"])
        _instrument("bytes_read", "counter", "By").add(stats["bytes_read"])
    except Exception:
        logger.debug("record_operation: _instrument failed (ignored)", exc_info=True)


def record_publication(outcome: str) -> None:
    try:
        _instrument("publications_total", "counter").add(1, {"outcome": outcome})
    except Exception:
        logger.debug("record_publication: _instrument failed (ignored)", exc_info=True)
