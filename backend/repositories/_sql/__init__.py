"""Named, parameterized SQL templates extracted out of inline repository strings.

Repository SQL templates live in their respective `backend/repositories/*.py`
modules (or `backend/repositories/insights/definitions.py`) and are aliased here
for backward-compatible imports (`from backend.repositories._sql import <module>`).
"""

from __future__ import annotations

import sys as _sys

from backend.repositories import alerts as _alerts_repo
from backend.repositories import cmcd as _cmcd_repo
from backend.repositories import dashboard as _dashboard_repo
from backend.repositories import network as _network_repo
from backend.repositories import origin as _origin_repo
from backend.repositories import performance as _performance_repo
from backend.repositories import query as _query_repo
from backend.repositories import rum as _rum_repo
from backend.repositories import session_scoring as _session_scoring_repo
from backend.repositories import sessions as _sessions_repo
from backend.repositories import usage as _usage_repo
from backend.repositories.insights import definitions as _insights_defs

_sys.modules[f"{__name__}.alerts"] = _alerts_repo
_sys.modules[f"{__name__}.cmcd"] = _cmcd_repo
_sys.modules[f"{__name__}.dashboard"] = _dashboard_repo
_sys.modules[f"{__name__}.insights"] = _insights_defs
_sys.modules[f"{__name__}.network"] = _network_repo
_sys.modules[f"{__name__}.origin"] = _origin_repo
_sys.modules[f"{__name__}.performance"] = _performance_repo
_sys.modules[f"{__name__}.query"] = _query_repo
_sys.modules[f"{__name__}.rum"] = _rum_repo
_sys.modules[f"{__name__}.session_scoring"] = _session_scoring_repo
_sys.modules[f"{__name__}.sessions"] = _sessions_repo
_sys.modules[f"{__name__}.usage"] = _usage_repo
