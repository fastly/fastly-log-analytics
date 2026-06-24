"""Unit tests for the opt-in Web Vitals JSONL sink."""

from __future__ import annotations

import json

import pytest

from backend.core import web_vitals_store


@pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "on", "On"])
def test_collection_enabled_truthy_values(monkeypatch, value):
    monkeypatch.setenv("WEB_VITALS_COLLECT", value)
    assert web_vitals_store.collection_enabled() is True


@pytest.mark.parametrize("value", ["0", "false", "no", "off", ""])
def test_collection_enabled_falsey_values(monkeypatch, value):
    monkeypatch.setenv("WEB_VITALS_COLLECT", value)
    assert web_vitals_store.collection_enabled() is False


def test_collection_disabled_when_unset(monkeypatch):
    monkeypatch.delenv("WEB_VITALS_COLLECT", raising=False)
    assert web_vitals_store.collection_enabled() is False


def test_append_sample_writes_one_json_line_each(monkeypatch, tmp_path):
    sink = tmp_path / "nested" / "web_vitals.jsonl"  # parent dir is created lazily
    monkeypatch.setattr(web_vitals_store, "LOG_PATH", sink)

    web_vitals_store.append_sample({"name": "LCP", "value": 1200.0, "rating": "good"})
    web_vitals_store.append_sample({"name": "CLS", "value": 0.05, "rating": "good"})

    lines = sink.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0])["name"] == "LCP"
    assert json.loads(lines[1])["name"] == "CLS"


def test_append_sample_never_raises_on_io_error(monkeypatch, tmp_path):
    # Point LOG_PATH at a path whose parent is a file, so mkdir/open fails;
    # the helper must swallow the error rather than break the request.
    blocker = tmp_path / "afile"
    blocker.write_text("x")
    monkeypatch.setattr(web_vitals_store, "LOG_PATH", blocker / "child" / "wv.jsonl")
    web_vitals_store.append_sample({"name": "LCP", "value": 1.0, "rating": "good"})  # no exception


def test_max_bytes_defaults_to_200mb(monkeypatch):
    monkeypatch.delenv("WEB_VITALS_MAX_MB", raising=False)
    assert web_vitals_store._max_bytes() == 200 * 1024 * 1024


def test_max_bytes_invalid_falls_back_to_default(monkeypatch):
    monkeypatch.setenv("WEB_VITALS_MAX_MB", "not-a-number")
    assert web_vitals_store._max_bytes() == 200 * 1024 * 1024


def test_max_bytes_zero_disables_cap(monkeypatch):
    monkeypatch.setenv("WEB_VITALS_MAX_MB", "0")
    assert web_vitals_store._max_bytes() == 0


def test_rotation_creates_single_backup_at_cap(monkeypatch, tmp_path):
    """Once the active file reaches the cap it's rotated to a single .1
    backup and a fresh file continues — recent data is retained, disk is
    bounded at ~2x the cap."""
    monkeypatch.setattr(web_vitals_store, "LOG_PATH", tmp_path / "wv.jsonl")
    # ~52-byte cap: smaller than one sample line, so each write past the
    # first rotates. Exercises the rotation path deterministically.
    monkeypatch.setenv("WEB_VITALS_MAX_MB", "0.00005")
    for i in range(4):
        web_vitals_store.append_sample({"name": "LCP", "value": i, "rating": "good"})

    active = tmp_path / "wv.jsonl"
    backup = web_vitals_store.rotated_path()
    assert active.exists()
    assert backup.exists()  # rotation happened
    # Only ONE backup is kept, and the active file holds the most recent
    # write(s) — not all four.
    assert len(active.read_text(encoding="utf-8").strip().splitlines()) < 4
    assert not (tmp_path / "wv.jsonl.2").exists()


def test_cap_zero_disables_rotation(monkeypatch, tmp_path):
    monkeypatch.setattr(web_vitals_store, "LOG_PATH", tmp_path / "wv.jsonl")
    monkeypatch.setenv("WEB_VITALS_MAX_MB", "0")
    for i in range(5):
        web_vitals_store.append_sample({"name": "LCP", "value": i, "rating": "good"})
    assert not web_vitals_store.rotated_path().exists()
    assert len((tmp_path / "wv.jsonl").read_text(encoding="utf-8").strip().splitlines()) == 5
