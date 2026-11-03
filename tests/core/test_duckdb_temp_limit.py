"""The DuckDB spill-directory cap must scale with free disk instead of a fixed 10 GB."""

from __future__ import annotations

import shutil
from collections import namedtuple

import pytest

from backend.core import duckdb as core_duckdb

_Usage = namedtuple("_Usage", "total used free")


def _free(monkeypatch: pytest.MonkeyPatch, free_bytes: int) -> None:
    monkeypatch.setattr(shutil, "disk_usage", lambda _p: _Usage(0, 0, free_bytes))


def test_cap_is_half_of_free_disk(monkeypatch, tmp_path):
    monkeypatch.delenv("DUCKDB_MAX_TEMP_DIRECTORY_SIZE", raising=False)
    _free(monkeypatch, 176 * 10**9)
    assert core_duckdb._max_temp_directory_size(str(tmp_path)) == "88GB"


def test_cap_never_drops_below_ten_gb(monkeypatch, tmp_path):
    monkeypatch.delenv("DUCKDB_MAX_TEMP_DIRECTORY_SIZE", raising=False)
    _free(monkeypatch, 4 * 10**9)
    assert core_duckdb._max_temp_directory_size(str(tmp_path)) == "10GB"


def test_env_override_wins(monkeypatch, tmp_path):
    monkeypatch.setenv("DUCKDB_MAX_TEMP_DIRECTORY_SIZE", "30GB")
    _free(monkeypatch, 500 * 10**9)
    assert core_duckdb._max_temp_directory_size(str(tmp_path)) == "30GB"


def test_unreadable_disk_falls_back_to_the_floor(monkeypatch, tmp_path):
    monkeypatch.delenv("DUCKDB_MAX_TEMP_DIRECTORY_SIZE", raising=False)

    def _boom(_p):
        raise OSError("no such device")

    monkeypatch.setattr(shutil, "disk_usage", _boom)
    assert core_duckdb._max_temp_directory_size(str(tmp_path / "missing" / "db.duckdb")) == "10GB"
