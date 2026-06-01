"""Shared pagination helpers for repositories."""

from __future__ import annotations


def calc_offset(page: int, limit: int) -> int:
    """Return the SQL OFFSET for 1-indexed `page` of `limit` rows.

    Clamps negative pages to 0 so callers can safely pass user-supplied
    values without an extra guard.
    """
    return max(0, (page - 1) * limit)
