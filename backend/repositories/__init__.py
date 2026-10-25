"""Repository layer for analytical queries and rollup planning."""

from __future__ import annotations

import sys as _sys

from backend.repositories import rollup_coverage as _rollup_coverage
from backend.repositories import top_n_rollups as _top_n_rollups

_sys.modules.setdefault(f"{__name__}._rollup_planner", _rollup_coverage)
_sys.modules.setdefault(f"{__name__}.ip_spread_rollups", _top_n_rollups)
