"""Filesystem path-traversal cage.

Single source of truth for "does this resolved path stay inside the allowed
base directory?" — the check that bounds user-influenced cache reads against
``../../etc/passwd`` and absolute-path payloads.
"""

from __future__ import annotations

import os


def path_within_dir(base_dir: str, candidate: str) -> bool:
    """Return ``True`` iff ``candidate`` resolves to a path inside ``base_dir``.

    Realpaths both sides (collapsing ``..`` and following symlinks) and
    requires their ``commonpath`` to equal the resolved base. Returns
    ``False`` — never raises — when the two share no common base (different
    drives, or mixed absolute/relative), so callers express their own reject
    shape (HTTP 400, ``ValueError``, skip-with-log, ...).
    """
    base_real = os.path.realpath(base_dir)
    candidate_real = os.path.realpath(candidate)
    try:
        return os.path.commonpath([base_real, candidate_real]) == base_real
    except ValueError:
        # commonpath raises when the paths have different drives / mixed
        # absolute-relative — treat as an escape.
        return False
