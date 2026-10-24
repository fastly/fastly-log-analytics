"""DuckLake data sync shim.

DuckLake writes Parquet directly to the local data root during commit,
so there is no separate cloud-to-local Parquet pull step. ``sync_data`` is
retained as a no-op shim for callers that invoke it during service sync.
"""

from __future__ import annotations

from typing import Any


def sync_data(
    source: dict,
    progress_callback: Any = None,
    start_time: str | None = None,
    end_time: str | None = None,
    table_name: str = "logs",
    **kwargs: Any,
) -> dict[str, int]:
    """No-op under DuckLake (files are written locally on commit)."""
    return {"files_downloaded": 0, "rows_downloaded": 0}
