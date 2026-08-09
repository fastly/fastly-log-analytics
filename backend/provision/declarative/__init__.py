"""Declarative VCL & Logging Management Engine.

Public API for the core declarative reconciliation system:
- FeatureState: Immutable configuration of desired service state
- reconcile_vcl_state: Main control loop for applying desired state to Fastly
- DiffResult: Computed diff between current and desired state
"""

from backend.provision.declarative.diff import DiffResult, compute_diff
from backend.provision.declarative.reconciler import reconcile_vcl_state
from backend.provision.declarative.state import FeatureState

__all__ = [
    "FeatureState",
    "DiffResult",
    "compute_diff",
    "reconcile_vcl_state",
]
