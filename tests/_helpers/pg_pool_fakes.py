"""A stand-in for the process-global metadata pool, for testing one-shot entry points without Postgres."""

from __future__ import annotations

from typing import Any

import pytest

from backend.core.metadata import pg_pool


class FakePgPool:
    def __init__(self, **_kwargs: Any) -> None:
        self.closed = False
        self.returned: list[object] = []

    def getconn(self) -> object:
        return object()

    def putconn(self, conn: object) -> None:
        self.returned.append(conn)

    def close(self, timeout: float = 5.0) -> None:
        self.closed = True


def install_fake_pg_pool(monkeypatch: pytest.MonkeyPatch) -> list[FakePgPool]:
    """Start with no global pool; every pool ``get_pg_pool`` opens afterwards is a recorded fake."""
    opened: list[FakePgPool] = []

    def open_pool(**kwargs: Any) -> FakePgPool:
        pool = FakePgPool(**kwargs)
        opened.append(pool)
        return pool

    monkeypatch.setenv("METADATA_DSN", "postgresql://fake@127.0.0.1:1/fake")
    monkeypatch.setattr(pg_pool, "ConnectionPool", open_pool)
    monkeypatch.setattr(pg_pool, "_pool", None)
    return opened
