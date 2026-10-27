"""Named, parameterized SQL templates extracted out of inline repository strings.

Repository SQL templates live in their respective `backend/repositories/*.py`
modules (or `backend/repositories/insights/definitions.py`) and are aliased here
for backward-compatible imports (`from backend.repositories._sql import <module>`).
"""

from __future__ import annotations

import sys as _sys

from backend.repositories import assets as _assets_repo
from backend.repositories import dashboard as _dashboard_repo
from backend.repositories import network as _network_repo
from backend.repositories import origin as _origin_repo
from backend.repositories import security as _security_repo
from backend.repositories import sessions as _sessions_repo
from backend.repositories.insights import definitions as _insights_defs

alerts = _assets_repo
cmcd = _origin_repo
dashboard = _dashboard_repo
insights = _insights_defs
network = _network_repo
origin = _origin_repo
performance = _origin_repo
query = _dashboard_repo
rum = _origin_repo
security = _security_repo
session_scoring = _sessions_repo
sessions = _sessions_repo
usage = _assets_repo

_sys.modules[f"{__name__}.alerts"] = _assets_repo
_sys.modules[f"{__name__}.cmcd"] = _origin_repo
_sys.modules[f"{__name__}.dashboard"] = _dashboard_repo
_sys.modules[f"{__name__}.insights"] = _insights_defs
_sys.modules[f"{__name__}.network"] = _network_repo
_sys.modules[f"{__name__}.origin"] = _origin_repo
_sys.modules[f"{__name__}.performance"] = _origin_repo
_sys.modules[f"{__name__}.query"] = _dashboard_repo
_sys.modules[f"{__name__}.rum"] = _origin_repo
_sys.modules[f"{__name__}.security"] = _security_repo
_sys.modules[f"{__name__}.session_scoring"] = _sessions_repo
_sys.modules[f"{__name__}.sessions"] = _sessions_repo
_sys.modules[f"{__name__}.usage"] = _assets_repo
