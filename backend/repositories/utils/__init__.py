from __future__ import annotations

import sys as _sys

from backend.repositories import _base
from backend.repositories._base import (
    FilterBuilder,
    FilterClause,
    SqlFragment,
    build_geo_select_clause,
    build_where_clause,
    calc_offset,
    filter_spec_attr,
    normalize_filter_key,
)

filters = _base
response_cache = _base
_sys.modules["backend.repositories.utils.filters"] = _base
_sys.modules.setdefault("backend.repositories.utils.response_cache", _base)

__all__ = [
    "FilterBuilder",
    "FilterClause",
    "SqlFragment",
    "build_geo_select_clause",
    "build_where_clause",
    "calc_offset",
    "filter_spec_attr",
    "filters",
    "normalize_filter_key",
    "response_cache",
]
