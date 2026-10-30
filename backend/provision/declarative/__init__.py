"""Declarative VCL & Logging Management Engine.

Public API for the core declarative reconciliation system:
- FeatureState: Immutable configuration of desired service state
- reconcile_vcl_state: Main control loop for applying desired state to Fastly
- DiffResult: Computed diff between current and desired state
"""

from backend.provision.declarative.reconciler import (
    DiffResult,
    FeatureState,
    compute_diff,
    reconcile_vcl_state,
)

__all__ = [
    "FeatureState",
    "DiffResult",
    "compute_diff",
    "reconcile_vcl_state",
]
