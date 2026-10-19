"""Unit tests for the shared COPY / weighted-average / cleanup helpers."""

from __future__ import annotations

from unittest.mock import patch

from backend.core.rollups._common import parquet_zstd_target, unlink_quiet, wavg


def test_wavg_renders_weighted_average_sql():
    assert wavg("p95_us", "requests") == "SUM(p95_us * requests) / NULLIF(SUM(requests), 0)"


def test_parquet_zstd_target_renders_copy_tail():
    assert parquet_zstd_target("/tmp/x.parquet") == "TO '/tmp/x.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)"


def test_unlink_quiet_removes_existing_file(tmp_path):
    f = tmp_path / "a.parquet"
    f.write_bytes(b"x")
    unlink_quiet(str(f))
    assert not f.exists()


def test_unlink_quiet_ignores_missing_file(tmp_path):
    unlink_quiet(str(tmp_path / "absent.parquet"))


def test_unlink_quiet_swallows_any_oserror(tmp_path):
    with patch("backend.core.rollups._common.os.remove", side_effect=PermissionError("denied")):
        unlink_quiet(str(tmp_path / "locked.parquet"))
