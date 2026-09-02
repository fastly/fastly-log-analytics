"""Helm chart render gates.

The load-bearing assertion here is the backend replica invariant. The
serving tier is single-pod-only — DuckDB takes a process-exclusive lock on
the per-service read-write `.duckdb` file, so a second backend pod sharing
the PVC fails every pool checkout with `Could not set lock on file` and
returns 503 on every data request (see
docs/adr/18-serving-tier-single-pod.md). The chart used to gate a backend
HPA on the same `autoscaling.enabled` flag as the worker KEDA ScaledObject,
so turning on worker autoscaling — the tier that DOES scale — also scaled
the backend to maxReplicas: 10. These tests pin the fix so it can't
silently regress.
"""

import subprocess

import yaml

CHART = "./deploy/chart/fastly-log-analytics/"


def _render(*set_args: str) -> list[dict]:
    cmd = ["helm", "template", "test-release", CHART]
    for arg in set_args:
        cmd += ["--set", arg]
    result = subprocess.run(cmd, capture_output=True, text=True)
    assert result.returncode == 0, f"helm template failed: {result.stderr}"
    return [doc for doc in yaml.safe_load_all(result.stdout) if doc]


def _deployment(docs: list[dict], component: str) -> dict:
    return next(doc for doc in docs if doc["kind"] == "Deployment" and component in doc["metadata"]["name"])


def _autoscaler_targets(docs: list[dict]) -> set[str]:
    """Names every HPA/ScaledObject points at. KEDA's scaleTargetRef carries
    only `name` (kind defaults to Deployment), so both shapes are read the
    same way."""
    return {
        doc["spec"]["scaleTargetRef"]["name"]
        for doc in docs
        if doc["kind"] in ("HorizontalPodAutoscaler", "ScaledObject")
    }


def test_helm_template():
    docs = _render()

    kinds = [doc["kind"] for doc in docs]
    assert "Deployment" in kinds
    assert "Service" in kinds
    assert "Ingress" in kinds

    assert _deployment(docs, "backend")["spec"]["replicas"] == 1
    assert _deployment(docs, "worker")["spec"]["replicas"] == 1


def test_backend_cannot_exceed_one_replica_under_default_values():
    docs = _render()
    backend = _deployment(docs, "backend")

    assert backend["spec"]["replicas"] == 1
    # Nothing may hand the backend Deployment to an autoscaler: an HPA would
    # override the pinned replica count at runtime.
    assert backend["metadata"]["name"] not in _autoscaler_targets(docs)


def test_backend_stays_single_pod_when_autoscaling_and_replicacount_are_raised():
    """The exact footgun this pins: an operator enables autoscaling to scale
    the worker fleet and bumps replicaCount, and the backend must not move."""
    docs = _render("autoscaling.enabled=true", "replicaCount=5", "autoscaling.minReplicas=3")
    backend = _deployment(docs, "backend")

    assert backend["spec"]["replicas"] == 1
    assert backend["metadata"]["name"] not in _autoscaler_targets(docs)


def test_worker_autoscaling_still_works():
    """The flip side — pinning the backend must not disarm the tier that DOES
    scale horizontally (ADR-15/ADR-16 ingest fleet)."""
    docs = _render("autoscaling.enabled=true")
    targets = _autoscaler_targets(docs)

    assert any("worker" in name for name in targets), targets
    assert [doc for doc in docs if doc["kind"] == "ScaledObject"]
