"""Declarative VCL & Logging Management Engine.

Public API for the core declarative reconciliation system:
- FeatureState: Immutable configuration of desired service state
- reconcile_vcl_state: Main control loop for applying desired state to Fastly
- DiffResult: Computed diff between current and desired state
"""

import sys

from backend.provision.declarative import reconciler as diff
from backend.provision.declarative import reconciler as iac_reconciler
from backend.provision.declarative.reconciler import DiffResult, compute_diff, reconcile_vcl_state
from backend.provision.declarative.state import FeatureState

sys.modules[f"{__name__}.diff"] = diff
sys.modules[f"{__name__}.iac_reconciler"] = iac_reconciler

__all__ = [
    "FeatureState",
    "DiffResult",
    "compute_diff",
    "diff",
    "iac_reconciler",
    "reconcile_vcl_state",
]
