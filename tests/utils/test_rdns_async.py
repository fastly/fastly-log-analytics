"""Tests for the Phase 1.4a async rdns resolver path
(`_do_lookup_async`, `_resolve_batch_async`, `_bulk_update_async`).

Mocks aiodns at the resolver level (no network). The aiodns DNSResolver
exposes `.gethostbyaddr(ip)` returning a NamedTuple-shaped object with
`.name` (PTR hostname) and `.query(name, type)` returning a list of records
with `.host`.

The sync `_do_lookup` path is covered by the existing `test_rdns_cache.py`
suite. This file covers the async paths the existing tests don't reach.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import aiodns
import pytest

from backend.utils import rdns_cache


@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    db = tmp_path / "rdns_cache.db"
    monkeypatch.setattr(rdns_cache, "_DB_PATH", Path(db))
    rdns_cache._init()
    yield db


# ── _do_lookup_async ──────────────────────────────────────────────────────────


def _ptr(name: str):
    """Build the gethostbyaddr-shaped object aiodns returns."""
    obj = MagicMock()
    obj.name = name
    return obj


def _forward_records(hosts: list[str]):
    """Build the list of objects aiodns query() returns."""
    return [MagicMock(host=h) for h in hosts]


def test_do_lookup_async_returns_resolved_when_ptr_succeeds_and_fcrdns_passes():
    resolver = MagicMock()
    resolver.gethostbyaddr = AsyncMock(return_value=_ptr("crawl.googlebot.com"))
    resolver.query_dns = AsyncMock(return_value=_forward_records(["66.249.66.1"]))

    sem = asyncio.Semaphore(5)
    host, status, fcrdns = asyncio.run(
        rdns_cache._do_lookup_async("66.249.66.1", resolver, sem),
    )
    assert host == "crawl.googlebot.com"
    assert status == "resolved"
    assert fcrdns is True


def test_do_lookup_async_marks_fcrdns_false_on_forward_mismatch():
    resolver = MagicMock()
    resolver.gethostbyaddr = AsyncMock(return_value=_ptr("crawl-fake.googlebot.com"))
    resolver.query_dns = AsyncMock(return_value=_forward_records(["66.249.66.99"]))

    sem = asyncio.Semaphore(5)
    host, status, fcrdns = asyncio.run(
        rdns_cache._do_lookup_async("66.249.66.1", resolver, sem),
    )
    assert host == "crawl-fake.googlebot.com"
    assert status == "resolved"
    assert fcrdns is False


def test_do_lookup_async_returns_nxdomain_on_ares_enotfound():
    resolver = MagicMock()
    resolver.gethostbyaddr = AsyncMock(
        side_effect=aiodns.error.DNSError(aiodns.error.ARES_ENOTFOUND, "not found"),
    )
    sem = asyncio.Semaphore(5)
    host, status, fcrdns = asyncio.run(
        rdns_cache._do_lookup_async("1.2.3.4", resolver, sem),
    )
    assert host is None
    assert status == "nxdomain"
    assert fcrdns is False


def test_do_lookup_async_returns_error_on_other_dns_errors():
    resolver = MagicMock()
    # Code 11 (ARES_ETIMEOUT) is "timeout" — not nxdomain.
    resolver.gethostbyaddr = AsyncMock(
        side_effect=aiodns.error.DNSError(aiodns.error.ARES_ETIMEOUT, "timeout"),
    )
    sem = asyncio.Semaphore(5)
    host, status, fcrdns = asyncio.run(
        rdns_cache._do_lookup_async("1.2.3.4", resolver, sem),
    )
    assert host is None
    assert status == "error"
    assert fcrdns is False


# ── _resolve_batch_async ─────────────────────────────────────────────────────


def test_resolve_batch_async_runs_concurrently_and_returns_map():
    """All IPs resolved concurrently; result map keyed by IP."""

    fake_lookups = {
        "1.1.1.1": ("one.example.com", "resolved", True),
        "2.2.2.2": ("two.example.com", "resolved", False),
        "3.3.3.3": (None, "nxdomain", False),
    }

    async def fake_lookup(ip, resolver, semaphore):
        return fake_lookups[ip]

    with patch("backend.utils.rdns_cache._do_lookup_async", side_effect=fake_lookup):
        results = asyncio.run(rdns_cache._resolve_batch_async(list(fake_lookups)))

    assert results == fake_lookups


def test_resolve_batch_async_swallows_exceptions_into_error_status():
    """A single lookup raising must not poison the whole batch."""

    async def fake_lookup(ip, resolver, semaphore):
        if ip == "1.1.1.1":
            raise RuntimeError("boom")
        return ("ok.example.com", "resolved", True)

    with patch("backend.utils.rdns_cache._do_lookup_async", side_effect=fake_lookup):
        results = asyncio.run(rdns_cache._resolve_batch_async(["1.1.1.1", "2.2.2.2"]))

    assert results["1.1.1.1"] == (None, "error", False)
    assert results["2.2.2.2"] == ("ok.example.com", "resolved", True)


def test_resolve_batch_async_returns_empty_for_empty_input():
    assert asyncio.run(rdns_cache._resolve_batch_async([])) == {}


# ── _bulk_update_async ────────────────────────────────────────────────────────


def test_bulk_update_async_writes_records_in_single_transaction():
    """End-to-end bulk update writes the rows to the SQLite file."""
    rdns_cache.enqueue(["1.1.1.1", "2.2.2.2"])

    records = [
        ("one.example.com", "resolved", 1, "2026-06-09T00:00:00Z", "1.1.1.1"),
        ("two.example.com", "resolved", 0, "2026-06-09T00:00:00Z", "2.2.2.2"),
    ]
    asyncio.run(rdns_cache._bulk_update_async(records))

    h1, s1, _ = rdns_cache.get_hostname("1.1.1.1")
    h2, s2, _ = rdns_cache.get_hostname("2.2.2.2")
    assert (h1, s1) == ("one.example.com", "resolved")
    assert (h2, s2) == ("two.example.com", "resolved")


def test_bulk_update_async_noop_on_empty_records():
    """Empty input is a no-op (doesn't open a connection)."""
    asyncio.run(rdns_cache._bulk_update_async([]))
