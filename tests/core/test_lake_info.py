"""Tests for ``backend.core.lake_info`` — DuckLake metadata fetch.

``fetch_lake_info`` is the single source of truth for the time range
and snapshot calendar the frontend's date pickers and DuckLake view
use. Every chart on the dashboard renders the wrong window if the
timestamp range is off.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

from tests.core.test_ducklake_layer import _make_source, _write_buffer

# ── DuckLake direct (v3 write-path cutover) ──────────────────────────────────

#
# The pyiceberg catalog these used to mock (`init_iceberg_table`) is
# permanently frozen post-DuckLake-cutover — see `backend/core/iceberg/
# _ducklake.py` module docstring. `_fetch_direct` now reads real DuckLake
# state, so these are integration tests against a real file-backed DuckLake
# catalog (via `_make_source`/`_write_buffer`/`_commit_buffer_impl` from
# `test_ducklake_layer.py`), not mocks of the pyiceberg Table API.


def test_iceberg_fallback_returns_table_info(tmp_path):
    """When the fast path fails (e.g. ``get_object`` raises NoSuchKey),
    the DuckLake fallback reads real committed lake state directly. Tiny
    commits may be inlined or materialized into a different number of files,
    so only require a non-empty file/row count here."""
    src = _make_source(tmp_path, f"lk{uuid.uuid4().hex[:8]}")
    ts = datetime(2026, 2, 1, tzinfo=UTC)
    _write_buffer(src, "batch_a.parquet", ts=ts, source_file="s3://b/raw/a.gz", n=1)
    _write_buffer(src, "batch_b.parquet", ts=ts + timedelta(seconds=1), source_file="s3://b/raw/b.gz", n=1)
    from backend.core.iceberg.buffer import _commit_buffer_impl

    assert _commit_buffer_impl(src)["rows_committed"] == 2

    with patch("backend.core.duckdb._get_fos_client", side_effect=RuntimeError("no s3")):
        from backend.core.iceberg.lake_info import fetch_lake_info

        out = fetch_lake_info(src)

    assert out["ok"] is True
    assert out["table_exists"] is True
    assert out["info"]["min_timestamp"] == "2026-02-01T00:00:00+00:00"
    assert out["info"]["data_files"] >= 1
    assert out["calendar"] == {"2026-02-01": {"data_files": 2, "size_bytes": 0}}
    assert out["range"]["start"] == "2026-02-01T00:00:00+00:00"


def test_iceberg_fallback_returns_table_does_not_exist_when_none(tmp_path):
    """A DuckLake catalog with no committed table yet (pre-first-ingest)
    must surface as ``table_exists: False`` so the UI shows the empty
    state instead of an error."""
    src = _make_source(tmp_path, f"lk{uuid.uuid4().hex[:8]}")

    with patch("backend.core.duckdb._get_fos_client", side_effect=RuntimeError("no s3")):
        from backend.core.iceberg.lake_info import fetch_lake_info

        out = fetch_lake_info(src)

    assert out == {"ok": True, "table_exists": False, "message": "DuckLake table not found."}


def test_iceberg_fallback_surfaces_unexpected_errors(tmp_path):
    """A real error (bad DuckLake attach) — NOT a missing-table — surfaces
    as ``ok: False`` with the error string. The frontend distinguishes
    this from the empty-lake case to render a different UI affordance."""
    src = _make_source(tmp_path, f"lk{uuid.uuid4().hex[:8]}")

    def _boom(con, source, read_only=False):
        raise RuntimeError("403 AccessDenied")

    with (
        patch("backend.core.duckdb._get_fos_client", side_effect=RuntimeError("no s3")),
        patch("backend.core.iceberg._ducklake._ducklake_attach", side_effect=_boom),
    ):
        from backend.core.iceberg.lake_info import fetch_lake_info

        out = fetch_lake_info(src)

    assert out["ok"] is False
    assert "403" in out["error"]


# ── DuckLake fallback: temp cache (use_temp_cache=True) ──────────────────────


def test_temp_cache_path_clears_source_caches_on_exit(tmp_path):
    """``use_temp_cache=True`` is used during PROVISIONING (the service
    isn't registered yet). After deriving info, it MUST call
    ``clear_source_caches`` — otherwise the in-memory catalog cache
    grows by one entry per provisioning attempt that gets aborted."""
    src = _make_source(tmp_path, f"lk{uuid.uuid4().hex[:8]}")
    ts = datetime(2026, 1, 1, tzinfo=UTC)
    _write_buffer(src, "batch.parquet", ts=ts, source_file="s3://b/raw/a.gz", n=1)
    from backend.core.iceberg.buffer import _commit_buffer_impl

    assert _commit_buffer_impl(src)["rows_committed"] == 1

    with (
        patch("backend.core.duckdb._get_fos_client", side_effect=RuntimeError("no s3")),
        patch("backend.core.iceberg.clear_source_caches") as mock_clear,
    ):
        from backend.core.iceberg.lake_info import fetch_lake_info

        out = fetch_lake_info(src, use_temp_cache=True)

    assert out["table_exists"] is True
    mock_clear.assert_called_once_with(src["name"])


def test_temp_cache_path_returns_empty_lake_when_table_missing(tmp_path):
    """Same empty-table coercion as the direct path, but routed through
    the temp_cache branch — pinned because both branches must agree
    on the shape they return to the provision wizard."""
    src = _make_source(tmp_path, f"lk{uuid.uuid4().hex[:8]}")

    with (
        patch("backend.core.duckdb._get_fos_client", side_effect=RuntimeError("no s3")),
        patch("backend.core.iceberg.clear_source_caches"),
    ):
        from backend.core.iceberg.lake_info import fetch_lake_info

        out = fetch_lake_info(src, use_temp_cache=True)

    assert out == {"ok": True, "table_exists": False, "message": "DuckLake table not found."}


def test_temp_cache_path_surfaces_unexpected_errors(tmp_path):
    """An unexpected error inside the temp_cache branch must surface
    via the outer except as ``ok: False``."""
    src = _make_source(tmp_path, f"lk{uuid.uuid4().hex[:8]}")

    def _boom(con, source, read_only=False):
        raise RuntimeError("403 boom")

    with (
        patch("backend.core.duckdb._get_fos_client", side_effect=RuntimeError("no s3")),
        patch("backend.core.iceberg._ducklake._ducklake_attach", side_effect=_boom),
        patch("backend.core.iceberg.clear_source_caches"),
    ):
        from backend.core.iceberg.lake_info import fetch_lake_info

        out = fetch_lake_info(src, use_temp_cache=True)

    assert out["ok"] is False
    assert "403" in out["error"]
