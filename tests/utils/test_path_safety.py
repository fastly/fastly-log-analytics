"""Tests for the filesystem path-traversal cage utilities."""

from __future__ import annotations

import os

from backend.utils.path_safety import path_within_dir


def test_path_within_dir_basic(tmp_path):
    base_dir = str(tmp_path)

    # 1. Path inside base directory
    candidate = str(tmp_path / "subdir" / "file.txt")
    assert path_within_dir(base_dir, candidate) is True

    # 2. Path equals base directory
    assert path_within_dir(base_dir, base_dir) is True

    # 3. Path outside base directory via relative traversal
    candidate_escape = str(tmp_path / ".." / "other_dir")
    assert path_within_dir(base_dir, candidate_escape) is False


def test_path_within_dir_value_error_fallback(monkeypatch):
    # Mock os.path.commonpath to raise ValueError to test the exception block coverage
    def mock_commonpath(paths):
        raise ValueError("Simulated drive mismatch or empty paths")

    monkeypatch.setattr(os.path, "commonpath", mock_commonpath)
    assert path_within_dir("/tmp/base", "/tmp/base/candidate") is False
