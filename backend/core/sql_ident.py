"""Single validator for bare SQL identifiers interpolated into statements.

DuckDB cannot bind identifiers, so aliases, table names and field names are
interpolated into SQL text. Every such site validates through this module
instead of carrying its own regex (Trap #4: validate at the point of
interpolation, fail loudly). Stdlib only, so ``core`` leaves can import it.
"""

from __future__ import annotations

import re

__all__ = ["SAFE_IDENT_RE", "is_safe_ident", "require_safe_ident"]

SAFE_IDENT_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def is_safe_ident(name: object) -> bool:
    """True when ``name`` is a non-empty ``[A-Za-z_][A-Za-z0-9_]*`` string."""
    return isinstance(name, str) and SAFE_IDENT_RE.fullmatch(name) is not None


def require_safe_ident(name: str, label: str) -> str:
    """Return ``name`` unchanged or raise ``ValueError(f"{label}: {name!r}")``."""
    if not is_safe_ident(name):
        raise ValueError(f"{label}: {name!r}")
    return name
