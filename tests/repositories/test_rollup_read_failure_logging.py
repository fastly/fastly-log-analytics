import logging

import duckdb
import pytest

from backend.repositories.rollup_executor.scalars import _ScalarsRollupMixin


class _Runner(_ScalarsRollupMixin):
    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    def execute(self, *_a, **_k):
        raise self._exc


def test_rollup_read_failure_is_logged_with_reason_and_falls_back(caplog):
    runner = _Runner(duckdb.OutOfMemoryException("out of memory (9.3 GiB/9.3 GiB used)"))
    with caplog.at_level(logging.WARNING, logger="backend.repositories._base"):
        assert runner._exec_rollup("SELECT 1", tag="origin_status_rollup_oom_test") is None
    assert "origin_status_rollup_oom_test" in caplog.text
    assert "OutOfMemoryException" in caplog.text


def test_interrupt_still_propagates():
    with pytest.raises(duckdb.InterruptException):
        _Runner(duckdb.InterruptException("stop"))._exec_rollup("SELECT 1", tag="t")
