from __future__ import annotations

import logging

import pytest

from backend.utils import log_throttle
from backend.utils.log_throttle import log_once_per


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    monkeypatch.setattr(log_throttle, "_last", {})


def test_first_call_passes_then_is_suppressed_within_the_interval(monkeypatch):
    t = [100.0]
    monkeypatch.setattr(log_throttle.time, "monotonic", lambda: t[0])
    assert log_once_per("k", 60) is True
    t[0] += 59
    assert log_once_per("k", 60) is False
    t[0] += 2
    assert log_once_per("k", 60) is True


def test_keys_are_independent():
    assert log_once_per("a", 60) is True
    assert log_once_per("b", 60) is True
    assert log_once_per("a", 60) is False


def test_key_table_is_bounded(monkeypatch):
    monkeypatch.setattr(log_throttle, "_MAX_KEYS", 8)
    for i in range(100):
        log_once_per(f"k{i}", 3600)
    assert len(log_throttle._last) <= 8


def test_a_persistent_failure_emits_one_traceback_not_one_per_row(caplog):
    logger = logging.getLogger("t")
    with caplog.at_level(logging.DEBUG, logger="t"):
        for _ in range(500):
            try:
                raise ValueError("bad row")
            except ValueError:
                if log_once_per("row-parse", 60):
                    logger.debug("row parse failed", exc_info=True)
    assert len([r for r in caplog.records if r.exc_info]) == 1
