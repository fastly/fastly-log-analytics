"""Lock stand-ins for rollup tests."""

from __future__ import annotations

from contextlib import contextmanager


@contextmanager
def noop_lock(_key):
    """Replace a keyed per-service lock (e.g. ``_hour_lock``) with a no-op context manager."""
    yield
