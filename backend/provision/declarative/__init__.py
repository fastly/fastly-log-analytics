"""Declarative VCL & Logging Management Engine.

Public API for the core declarative reconciliation system:
- FeatureState: Immutable configuration of desired service state
- reconcile_vcl_state: Main control loop for applying desired state to Fastly
- DiffResult: Computed diff between current and desired state
"""

import sys

from backend.provision.declarative import reconciler as diff
from backend.provision.declarative import reconciler as iac_reconciler
from backend.provision.declarative import reconciler as state
from backend.provision.declarative.reconciler import (
    DiffResult,
    FeatureState,
    compute_diff,
    reconcile_vcl_state,
)

sys.modules[f"{__name__}.diff"] = diff
sys.modules[f"{__name__}.iac_reconciler"] = iac_reconciler
sys.modules[f"{__name__}.state"] = state
sys.modules[f"{__name__}.config_migration"] = state
_backend_pkg = sys.modules.get("backend")
if _backend_pkg is not None and hasattr(_backend_pkg, "_MODULE_ALIASES"):
    _backend_pkg._MODULE_ALIASES.setdefault("backend.provision.declarative.state", f"{__name__}.reconciler")
    _backend_pkg._MODULE_ALIASES.setdefault("backend.provision.declarative.config_migration", f"{__name__}.reconciler")

__all__ = [
    "FeatureState",
    "DiffResult",
    "compute_diff",
    "diff",
    "iac_reconciler",
    "reconcile_vcl_state",
    "state",
]
