"""Helm chart render gates.

Two invariants live here.

**The backend replica invariant.** The serving tier is single-pod-only —
DuckDB takes a process-exclusive lock on the per-service read-write
`.duckdb` file, so a second backend pod sharing the PVC fails every pool
checkout with `Could not set lock on file` and returns 503 on every data
request (see docs/adr/18-serving-tier-single-pod.md). The chart used to gate
a backend HPA on the same `autoscaling.enabled` flag as the worker KEDA
ScaledObject, so turning on worker autoscaling — the tier that DOES scale —
also scaled the backend to maxReplicas: 10.

**The installable-defaults invariant.** `values.yaml` used to ship
`config.ingestMode: celery` with both required Postgres DSNs empty, so a
`helm install` with no `--set` flags deployed a backend, worker and beat that
`config.validate_ingest_mode()` correctly refused to start — three
CrashLoopBackOffs whose cause was only visible in pod logs. The chart now
defaults to the single-node `sync` topology, which needs no external
datastores, and celery mode is an opt-in that fails at TEMPLATE time naming
each missing value. These tests pin both directions: defaults render a
self-contained install, and every incoherent celery configuration is a
`helm template` error.
"""

import subprocess

import yaml

CHART = "./deploy/chart/fastly-log-analytics/"

_PG = "postgresql://fla:pw@postgres:5432/ducklake"

# The minimum an operator must supply to select the scaled topology. Kept as
# one constant so a new requirement shows up as one edit here rather than as
# a scatter of --set flags.
CELERY = (
    "config.ingestMode=celery",
    "config.schedulerMode=external",
    "config.sseBackplane=valkey",
    f"config.ducklakeCatalog={_PG}",
    f"secrets.metadataDsn={_PG}",
    "secrets.celeryBrokerUrl=redis://valkey-master:6379/0",
)


def _helm(*set_args: str) -> subprocess.CompletedProcess:
    cmd = ["helm", "template", "test-release", CHART]
    for arg in set_args:
        cmd += ["--set", arg]
    return subprocess.run(cmd, capture_output=True, text=True)


def _render(*set_args: str) -> list[dict]:
    result = _helm(*set_args)
    assert result.returncode == 0, f"helm template failed: {result.stderr}"
    return [doc for doc in yaml.safe_load_all(result.stdout) if doc]


def _render_error(*set_args: str) -> str:
    """Assert the render FAILS and hand back the operator-facing message."""
    result = _helm(*set_args)
    assert result.returncode != 0, f"expected helm template to fail, got:\n{result.stdout}"
    return result.stderr


def _deployment(docs: list[dict], component: str) -> dict:
    return next(doc for doc in docs if doc["kind"] == "Deployment" and component in doc["metadata"]["name"])


def _deployments(docs: list[dict]) -> set[str]:
    return {doc["metadata"]["name"] for doc in docs if doc["kind"] == "Deployment"}


def _env(deployment: dict) -> dict[str, dict]:
    """name -> the whole env entry, so both `value` and `valueFrom` are visible."""
    container = deployment["spec"]["template"]["spec"]["containers"][0]
    return {entry["name"]: entry for entry in container.get("env", [])}


def _autoscaler_targets(docs: list[dict]) -> set[str]:
    """Names every HPA/ScaledObject points at. KEDA's scaleTargetRef carries
    only `name` (kind defaults to Deployment), so both shapes are read the
    same way."""
    return {
        doc["spec"]["scaleTargetRef"]["name"]
        for doc in docs
        if doc["kind"] in ("HorizontalPodAutoscaler", "ScaledObject")
    }


# ── Installable defaults ──────────────────────────────────────────────────────


def test_helm_template():
    docs = _render()

    kinds = [doc["kind"] for doc in docs]
    assert "Deployment" in kinds
    assert "Service" in kinds
    assert "Ingress" in kinds

    assert _deployment(docs, "backend")["spec"]["replicas"] == 1


def test_default_values_render_a_self_contained_install():
    """A `helm install` with no --set flags must be deployable as rendered:
    single-node ingest, no Postgres, no broker, no Celery pods. The failure
    this pins is the shipped-defaults CrashLoop — celery mode with both
    required DSNs empty."""
    docs = _render()
    env = _env(_deployment(docs, "backend"))

    assert env["INGEST_MODE"]["value"] == "sync"
    assert env["SCHEDULER_MODE"]["value"] == "inprocess"
    assert env["SSE_BACKPLANE"]["value"] == "local"

    # No datastore wiring at all: an empty DUCKLAKE_CATALOG or a secretKeyRef
    # into a Secret that was never templated is exactly how this broke.
    assert "DUCKLAKE_CATALOG" not in env
    assert "CELERY_BROKER_URL" not in env
    assert "METADATA_DSN" not in env
    assert not [doc for doc in docs if doc["kind"] == "Secret"]

    # No worker/beat pods to sit in CrashLoopBackOff against a broker that
    # the default install does not have.
    names = _deployments(docs)
    assert not [name for name in names if "worker" in name or "beat" in name], names


def _assert_boot_gate_would_pass(docs: list[dict]) -> None:
    """Mirror of backend/config.py::validate_ingest_mode() over the rendered
    pod specs. The real gate runs in the backend lifespan AND in
    worker_process_init, so every pod carrying INGEST_MODE=celery has to
    satisfy it or it CrashLoops."""
    for doc in docs:
        if doc["kind"] != "Deployment":
            continue
        env = _env(doc)
        if env.get("INGEST_MODE", {}).get("value") != "celery":
            continue
        name = doc["metadata"]["name"]
        assert env["DUCKLAKE_CATALOG"]["value"].startswith(("postgres://", "postgresql://")), name
        assert "METADATA_DSN" in env, name
        assert "CELERY_BROKER_URL" in env, name


def test_no_rendered_pod_would_fail_the_app_boot_gate():
    _assert_boot_gate_would_pass(_render())
    _assert_boot_gate_would_pass(_render(*CELERY))


# ── Celery mode is a validated opt-in ────────────────────────────────────────


def test_celery_without_ducklake_catalog_fails_at_template_time():
    stderr = _render_error("config.ingestMode=celery")

    assert "config.ducklakeCatalog" in stderr
    assert "postgresql://" in stderr
    # Names the escape hatch, not just the problem.
    assert "config.ingestMode=sync" in stderr


def test_celery_without_metadata_dsn_fails_at_template_time():
    stderr = _render_error("config.ingestMode=celery", f"config.ducklakeCatalog={_PG}")

    assert "secrets.metadataDsn" in stderr
    assert "secrets.existingSecret" in stderr


def test_celery_without_broker_fails_at_template_time():
    stderr = _render_error(
        "config.ingestMode=celery",
        f"config.ducklakeCatalog={_PG}",
        f"secrets.metadataDsn={_PG}",
    )

    assert "secrets.celeryBrokerUrl" in stderr


def test_celery_rejects_a_file_ducklake_catalog():
    """A file catalog is single-process; the boot gate rejects it, so the
    template must too rather than deferring to a CrashLoop."""
    stderr = _render_error(
        "config.ingestMode=celery",
        "config.ducklakeCatalog=/app/data/catalog.ducklake",
    )

    assert "config.ducklakeCatalog" in stderr
    assert "/app/data/catalog.ducklake" in stderr


def test_celery_rejects_a_non_postgres_metadata_dsn():
    stderr = _render_error(
        "config.ingestMode=celery",
        f"config.ducklakeCatalog={_PG}",
        "secrets.metadataDsn=sqlite:///app/data/metadata.db",
    )

    assert "secrets.metadataDsn" in stderr


def test_unrecognised_ingest_mode_fails_at_template_time():
    """The backend treats any non-"celery" value as sync, so a typo would
    silently disable the whole ingest fleet."""
    stderr = _render_error("config.ingestMode=Celery")

    assert "config.ingestMode" in stderr
    assert "Celery" in stderr


def test_external_scheduler_outside_celery_mode_fails_at_template_time():
    """SCHEDULER_MODE=external routes the discovery/ingest family to RedBeat;
    with no worker fleet consuming it, ingest stops and the scheduler still
    reports success."""
    stderr = _render_error("config.schedulerMode=external")

    assert "config.schedulerMode=external" in stderr
    assert "config.ingestMode=celery" in stderr


def test_valkey_backplane_without_a_broker_fails_at_template_time():
    stderr = _render_error("config.sseBackplane=valkey")

    assert "secrets.celeryBrokerUrl" in stderr


def test_celery_mode_renders_the_full_ingest_fleet():
    docs = _render(*CELERY)
    names = _deployments(docs)

    assert [name for name in names if "worker" in name], names
    assert [name for name in names if "beat" in name], names

    for component in ("backend", "worker", "beat"):
        env = _env(_deployment(docs, component))
        assert env["INGEST_MODE"]["value"] == "celery"
        assert env["DUCKLAKE_CATALOG"]["value"] == _PG
        # Non-optional: a Secret missing the key must stop the pod with
        # "couldn't find key" rather than boot it into the config gate.
        assert env["METADATA_DSN"]["valueFrom"]["secretKeyRef"]["optional"] is False
        assert env["CELERY_BROKER_URL"]["valueFrom"]["secretKeyRef"]["optional"] is False

    secret = next(doc for doc in docs if doc["kind"] == "Secret")
    assert secret["stringData"]["METADATA_DSN"] == _PG
    assert secret["stringData"]["CELERY_BROKER_URL"] == "redis://valkey-master:6379/0"


def test_existing_secret_satisfies_the_dsn_requirement():
    """The production path: connection strings pre-created out of band, so
    the chart templates no Secret and cannot inspect the keys."""
    docs = _render(
        "config.ingestMode=celery",
        f"config.ducklakeCatalog={_PG}",
        "secrets.existingSecret=fla-connections",
    )

    assert not [doc for doc in docs if doc["kind"] == "Secret"]
    env = _env(_deployment(docs, "backend"))
    assert env["METADATA_DSN"]["valueFrom"]["secretKeyRef"]["name"] == "fla-connections"


# ── Backend single-replica invariant (ADR-18) ────────────────────────────────


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


def test_backend_stays_single_pod_in_celery_mode_too():
    """Celery mode is the topology an operator reaches for to scale out, so
    the replica pin has to hold there, not only under the defaults."""
    docs = _render(*CELERY, "autoscaling.enabled=true", "replicaCount=5", "workers.replicaCount=4")
    backend = _deployment(docs, "backend")

    assert backend["spec"]["replicas"] == 1
    assert backend["metadata"]["name"] not in _autoscaler_targets(docs)
    # And the tier that DOES scale actually did.
    assert _deployment(docs, "worker")["spec"]["replicas"] == 4


def test_worker_autoscaling_still_works():
    """The flip side — pinning the backend must not disarm the tier that DOES
    scale horizontally (ADR-15/ADR-16 ingest fleet)."""
    docs = _render(*CELERY, "autoscaling.enabled=true")
    targets = _autoscaler_targets(docs)

    assert any("worker" in name for name in targets), targets
    assert [doc for doc in docs if doc["kind"] == "ScaledObject"]


def test_no_worker_scaledobject_without_a_worker_fleet():
    """In sync mode there is no worker Deployment, so a ScaledObject would
    target a workload that does not exist."""
    docs = _render("autoscaling.enabled=true")

    assert not [doc for doc in docs if doc["kind"] == "ScaledObject"]
    assert all("worker" not in name for name in _autoscaler_targets(docs))
