"""Helm chart render gates.

Two invariants live here.

**The backend replica invariant.** The chart keeps the backend at one replica
until all pod-local accelerators are shared. Sync/file serving still relies on
DuckDB's process-exclusive per-service file lock; durable Celery serving uses
ephemeral read-only connections but does not yet make rollups and other
accelerators fleet-wide (see docs/adr/18-serving-tier-single-pod.md). The chart
used to gate a backend HPA on the same `autoscaling.enabled` flag as the
worker KEDA ScaledObject, so turning on worker autoscaling — the tier that
DOES scale — also scaled the backend to maxReplicas: 10.

**The installable-defaults invariant.** `values.yaml` used to ship
`config.deploymentMode: high_throughput` with both required Postgres DSNs
empty, so a `helm install` with no `--set` flags deployed a backend, worker
and beat that `config.validate_deployment_mode()` correctly refused to start — three
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

STANDARD = (
    f"config.ducklakeCatalog={_PG}",
    f"secrets.metadataDsn={_PG}",
)

# The minimum an operator must supply to select the scaled topology. Kept as
# one constant so a new requirement shows up as one edit here rather than as
# a scatter of --set flags.
CELERY = (
    "config.deploymentMode=high_scale",
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
    docs = _render(*STANDARD)

    kinds = [doc["kind"] for doc in docs]
    assert "Deployment" in kinds
    assert "Service" in kinds
    assert "Ingress" in kinds
    assert "PersistentVolumeClaim" in kinds

    pvc = next(doc for doc in docs if doc["kind"] == "PersistentVolumeClaim")
    assert "storageClassName" not in pvc["spec"]
    assert pvc["spec"]["resources"]["requests"]["storage"] == "20Gi"

    assert _deployment(docs, "backend")["spec"]["replicas"] == 1


def test_default_values_render_a_self_contained_install():
    """Standard mode with Postgres DSNs renders single-node ingest, no broker,
    and no Celery worker pods."""
    docs = _render(*STANDARD)
    env = _env(_deployment(docs, "backend"))

    assert env["DEPLOYMENT_MODE"]["value"] == "standard"
    assert "INGEST_MODE" not in env
    assert "SERVING_MODE" not in env
    assert env["SSE_BACKPLANE"]["value"] == "local"

    assert env["DUCKLAKE_CATALOG"]["value"] == _PG
    assert "CELERY_BROKER_URL" in env
    assert env["METADATA_DSN"]["valueFrom"]["secretKeyRef"]["optional"] is False
    secret = next(doc for doc in docs if doc["kind"] == "Secret")
    assert secret["stringData"]["METADATA_DSN"] == _PG
    assert "CELERY_BROKER_URL" not in secret["stringData"]

    # No worker pods to sit in CrashLoopBackOff against a broker that
    # the default install does not have.
    names = _deployments(docs)
    assert not [name for name in names if "worker" in name], names


def test_standard_without_postgres_dsns_fails_at_template_time():
    """ADR-22 requires Postgres DSNs in standard mode too."""
    stderr = _render_error()
    assert "config.ducklakeCatalog" in stderr
    assert "postgresql://" in stderr

    stderr = _render_error(f"config.ducklakeCatalog={_PG}")
    assert "secrets.metadataDsn" in stderr
    assert "secrets.existingSecret" in stderr


def _assert_boot_gate_would_pass(docs: list[dict]) -> None:
    """Mirror of backend/config.py::validate_deployment_mode() over the rendered
    pod specs. The real gate runs in the backend lifespan AND in
    worker_process_init, so every pod carrying DEPLOYMENT_MODE=standard or
    high_scale has to satisfy it or it CrashLoops."""
    for doc in docs:
        if doc["kind"] != "Deployment":
            continue
        env = _env(doc)
        mode = env.get("DEPLOYMENT_MODE", {}).get("value")
        if mode not in ("standard", "high_scale"):
            continue
        name = doc["metadata"]["name"]
        assert "INGEST_MODE" not in env, name
        assert "SERVING_MODE" not in env, name
        assert env["DUCKLAKE_CATALOG"]["value"].startswith(("postgres://", "postgresql://")), name
        assert "METADATA_DSN" in env, name
        if mode == "high_scale":
            assert "CELERY_BROKER_URL" in env, name


def test_no_rendered_pod_would_fail_the_app_boot_gate():
    _assert_boot_gate_would_pass(_render(*STANDARD))
    _assert_boot_gate_would_pass(_render(*CELERY))


# ── Celery mode is a validated opt-in ────────────────────────────────────────


def test_high_throughput_fails_at_template_time():
    stderr = _render_error("config.deploymentMode=high_throughput")
    assert 'config.deploymentMode=high_throughput has been removed; use "standard" or "high_scale"' in stderr


def test_celery_without_ducklake_catalog_fails_at_template_time():
    stderr = _render_error("config.deploymentMode=high_scale")

    assert "config.ducklakeCatalog" in stderr
    assert "postgresql://" in stderr


def test_celery_without_metadata_dsn_fails_at_template_time():
    stderr = _render_error(
        "config.deploymentMode=high_scale",
        f"config.ducklakeCatalog={_PG}",
    )

    assert "secrets.metadataDsn" in stderr
    assert "secrets.existingSecret" in stderr


def test_celery_without_broker_fails_at_template_time():
    stderr = _render_error(
        "config.deploymentMode=high_scale",
        "config.sseBackplane=valkey",
        f"config.ducklakeCatalog={_PG}",
        f"secrets.metadataDsn={_PG}",
    )

    assert "secrets.celeryBrokerUrl" in stderr


def test_celery_rejects_a_file_ducklake_catalog():
    """A file catalog is single-process; the boot gate rejects it, so the
    template must too rather than deferring to a CrashLoop."""
    stderr = _render_error(
        "config.deploymentMode=high_scale",
        "config.ducklakeCatalog=/app/data/catalog.ducklake",
    )

    assert "config.ducklakeCatalog" in stderr
    assert "/app/data/catalog.ducklake" in stderr


def test_celery_rejects_a_non_postgres_metadata_dsn():
    stderr = _render_error(
        "config.deploymentMode=high_scale",
        f"config.ducklakeCatalog={_PG}",
        "secrets.metadataDsn=sqlite:///app/data/metadata.db",
    )

    assert "secrets.metadataDsn" in stderr


def test_unrecognised_deployment_mode_fails_at_template_time():
    """A typo in the deployment mode must fail at template time rather than
    silently disable the whole ingest fleet."""
    stderr = _render_error("config.deploymentMode=Celery")

    assert "config.deploymentMode" in stderr
    assert "Celery" in stderr


def test_valkey_backplane_without_a_broker_fails_at_template_time():
    stderr = _render_error("config.sseBackplane=valkey")

    assert "secrets.celeryBrokerUrl" in stderr


def test_celery_mode_renders_the_full_ingest_fleet():
    docs = _render(*CELERY)
    names = _deployments(docs)

    assert [name for name in names if "worker" in name], names

    for component in ("backend", "worker"):
        env = _env(_deployment(docs, component))
        assert env["DEPLOYMENT_MODE"]["value"] == "high_scale"
        assert "INGEST_MODE" not in env
        assert "SERVING_MODE" not in env
        assert env["DUCKLAKE_CATALOG"]["value"] == _PG
        # Non-optional: a Secret missing the key must stop the pod with
        # "couldn't find key" rather than boot it into the config gate.
        assert env["METADATA_DSN"]["valueFrom"]["secretKeyRef"]["optional"] is False
        assert "CELERY_BROKER_URL" in env
        assert env["DUCKDB_EXTENSION_DIRECTORY"]["value"] == "/tmp/duckdb-extensions"

    secret = next(doc for doc in docs if doc["kind"] == "Secret")
    assert secret["stringData"]["METADATA_DSN"] == _PG
    assert secret["stringData"]["CELERY_BROKER_URL"] == "redis://valkey-master:6379/0"


def test_backend_and_worker_share_duckdb_and_otel_env():
    for observability, exporter in (("false", "prometheus"), ("true", "otlp")):
        docs = _render(*CELERY, f"observability.enabled={observability}", "config.duckdbThreads=3")
        for component in ("backend", "worker"):
            env = _env(_deployment(docs, component))
            assert env["DUCKDB_MEMORY_LIMIT"]["value"] == "4GB"
            assert env["DUCKDB_THREADS"]["value"] == "3"
            assert env["OTEL_EXPORTER"]["value"] == exporter
            assert ("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT" in env) == (exporter == "otlp")
            assert ("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT" in env) == (exporter == "otlp")
            if exporter == "otlp":
                assert env["OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"]["value"] == (
                    "http://prometheus:9090/api/v1/otlp/v1/metrics"
                )
                assert env["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"]["value"] == "http://tempo:4318/v1/traces"

        if exporter == "otlp":
            service_names = {doc["metadata"]["name"] for doc in docs if doc["kind"] == "Service"}
            assert {"prometheus", "tempo"} <= service_names

    backend = _env(_deployment(_render(*CELERY), "backend"))
    worker = _env(_deployment(_render(*CELERY), "worker"))
    assert "DUCKDB_POOL_MAX_SIZE" in backend
    assert "DUCKDB_POOL_MAX_SIZE" not in worker


def test_operator_otlp_endpoints_render_for_high_scale_backend_and_worker():
    metrics = "metrics-endpoint-from-operator"
    traces = "traces-endpoint-from-operator"
    docs = _render(
        *CELERY,
        "otel.exporter=otlp",
        f"otel.metricsEndpoint={metrics}",
        f"otel.tracesEndpoint={traces}",
    )
    for component in ("backend", "worker"):
        env = _env(_deployment(docs, component))
        assert env["OTEL_EXPORTER"]["value"] == "otlp"
        assert env["OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"]["value"] == metrics
        assert env["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"]["value"] == traces


def test_otlp_exporter_requires_both_operator_destinations():
    error = _render_error(*CELERY, "otel.exporter=otlp")
    assert "requires an operator-provided otel.metricsEndpoint" in error

    error = _render_error(
        *CELERY,
        "otel.exporter=otlp",
        "otel.metricsEndpoint=metrics-endpoint-from-operator",
    )
    assert "requires an operator-provided otel.tracesEndpoint" in error


def test_custom_local_hosts_appends_to_backend_hosts():
    docs = _render(*STANDARD, "config.localHosts=backend-svc\\,frontend-svc")
    env = _env(_deployment(docs, "backend"))
    assert "backend-svc,frontend-svc" in env["LOCAL_HOSTS"]["value"]
    assert "test-release-fastly-log-analytics-backend" in env["LOCAL_HOSTS"]["value"]


def test_existing_secret_satisfies_the_dsn_requirement():
    """The production path: connection strings pre-created out of band, so
    the chart templates no Secret and cannot inspect the keys."""
    docs = _render(
        "config.deploymentMode=high_scale",
        f"config.ducklakeCatalog={_PG}",
        "secrets.existingSecret=fla-connections",
    )

    assert not [doc for doc in docs if doc["kind"] == "Secret"]
    env = _env(_deployment(docs, "backend"))
    assert env["METADATA_DSN"]["valueFrom"]["secretKeyRef"]["name"] == "fla-connections"


# ── Backend single-replica invariant (ADR-18) ────────────────────────────────


def test_backend_cannot_exceed_one_replica_under_default_values():
    docs = _render(*STANDARD)
    backend = _deployment(docs, "backend")

    assert backend["spec"]["replicas"] == 1
    # Nothing may hand the backend Deployment to an autoscaler: an HPA would
    # override the pinned replica count at runtime.
    assert backend["metadata"]["name"] not in _autoscaler_targets(docs)


def test_backend_stays_single_pod_when_autoscaling_and_replicacount_are_raised():
    """The exact footgun this pins: an operator enables autoscaling to scale
    the worker fleet and bumps replicaCount, and the backend must not move."""
    docs = _render(*STANDARD, "autoscaling.enabled=true", "replicaCount=5", "autoscaling.minReplicas=3")
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
    docs = _render(*STANDARD, "autoscaling.enabled=true")

    assert not [doc for doc in docs if doc["kind"] == "ScaledObject"]
    assert all("worker" not in name for name in _autoscaler_targets(docs))


# ── Optional high-scale application packaging ───────────────────────────────


def _high_scale_docs(*extra: str) -> list[dict]:
    return _render(
        f"config.ducklakeCatalog={_PG}",
        "secrets.existingSecret=fla-connections",
        "highScale.enabled=true",
        "highScale.sharedState.existingClaim=shared-app-state",
        *extra,
    )


def test_high_scale_is_disabled_without_opt_in():
    docs = _render(*STANDARD)
    assert not [doc for doc in docs if "high-scale" in doc["metadata"]["name"]]
    assert {doc["metadata"]["name"] for doc in docs if doc["kind"] == "Deployment"} == {
        "test-release-fastly-log-analytics-backend",
        "test-release-fastly-log-analytics-frontend",
    }


def test_high_scale_rejects_missing_shared_storage():
    error = _render_error(*STANDARD, "highScale.enabled=true")
    assert "require sharedState.existingClaim" in error


def test_high_scale_renders_only_application_workloads_and_invariants():
    docs = _high_scale_docs(
        "highScale.image.repository=example.invalid/fla-high-scale",
        "highScale.image.tag=test",
        "highScale.secret.existingName=fla-high-scale-connections",
    )
    deployments = {
        doc["metadata"]["name"]: doc
        for doc in docs
        if doc["kind"] == "Deployment" and "-high-scale-" in doc["metadata"]["name"]
    }
    assert {doc["metadata"]["labels"]["app.kubernetes.io/component"] for doc in deployments.values()} == {"ingest"}
    assert len([doc for doc in docs if doc["kind"] == "PodDisruptionBudget"]) == 1
    assert len([doc for doc in docs if doc["kind"] == "NetworkPolicy"]) == 1
    assert not [doc for doc in docs if doc["kind"] in {"ClickHouseCluster", "KeeperCluster"}]
    assert not [doc for doc in docs if doc["kind"] == "Secret"]

    for deployment in deployments.values():
        pod = deployment["spec"]["template"]["spec"]
        assert pod["serviceAccountName"].endswith("-high-scale")
        assert pod["containers"][0]["image"] == "example.invalid/fla-high-scale:test"
        assert pod["containers"][0]["resources"]["requests"]["cpu"] == "250m"
        assert pod["containers"][0]["securityContext"]["readOnlyRootFilesystem"] is True
        # Packaging must not silently select an application runtime mode.
        assert not any(entry["name"] == "DEPLOYMENT_MODE" for entry in pod["containers"][0].get("env", []))
        assert pod["containers"][0]["envFrom"][0]["secretRef"]["name"] == "fla-high-scale-connections"
        assert {
            "name": "shared-state",
            "mountPath": "/app/data",
            "subPath": "data",
        } in pod["containers"][0]["volumeMounts"]
        assert pod["volumes"] == [
            {"name": "tmp", "emptyDir": {}},
            {
                "name": "shared-state",
                "persistentVolumeClaim": {"claimName": "shared-app-state"},
            },
        ]


def test_high_scale_ingest_runs_continuous_worker_without_placeholder_workloads():
    docs = _high_scale_docs(
        "highScale.secret.existingName=fla-high-scale-connections",
    )
    deployments = {
        doc["metadata"]["name"]: doc
        for doc in docs
        if doc["kind"] == "Deployment" and "-high-scale-" in doc["metadata"]["name"]
    }

    assert {doc["metadata"]["labels"]["app.kubernetes.io/component"] for doc in deployments.values()} == {"ingest"}
    ingest = next(iter(deployments.values()))
    assert ingest["spec"]["template"]["spec"]["containers"][0]["command"] == [
        "python",
        "-m",
        "backend.high_scale.worker",
    ]
    worker_env = {entry["name"]: entry for entry in ingest["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert worker_env["HIGH_SCALE_WORKER_ID"] == {
        "name": "HIGH_SCALE_WORKER_ID",
        "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}},
    }
    assert {"name": "tmp", "mountPath": "/tmp"} in ingest["spec"]["template"]["spec"]["containers"][0]["volumeMounts"]
    assert {"name": "tmp", "emptyDir": {}} in ingest["spec"]["template"]["spec"]["volumes"]


def test_high_scale_rejects_unimplemented_workload_without_command():
    error = _render_error(
        *STANDARD,
        "highScale.enabled=true",
        "highScale.workloads.archive.enabled=true",
        "highScale.secret.existingName=fla-high-scale-connections",
    )
    assert "high-scale workload archive requires an explicit command" in error


def test_high_scale_can_disable_a_workload_and_secret_creation_is_explicit():
    docs = _high_scale_docs(
        "highScale.workloads.replay.enabled=false",
        "highScale.secret.create=true",
        "highScale.secret.stringData.CLICKHOUSE_URL=external-service",
    )
    assert len([doc for doc in docs if doc["kind"] == "Deployment" and "-high-scale-" in doc["metadata"]["name"]]) == 1
    secret = next(doc for doc in docs if doc["kind"] == "Secret")
    assert secret["stringData"]["CLICKHOUSE_URL"] == "external-service"


# ── ClickHouse Helm Chart Gates ───────────────────────────────────────────────

CHART_CLICKHOUSE = "./deploy/chart/clickhouse/"


def _helm_clickhouse(*set_args: str) -> subprocess.CompletedProcess:
    cmd = ["helm", "template", "test-release", CHART_CLICKHOUSE]
    for arg in set_args:
        cmd += ["--set", arg]
    return subprocess.run(cmd, capture_output=True, text=True)


def _render_clickhouse(*set_args: str) -> list[dict]:
    result = _helm_clickhouse(*set_args)
    assert result.returncode == 0, f"helm template failed: {result.stderr}"
    return [doc for doc in yaml.safe_load_all(result.stdout) if doc]


def _render_clickhouse_error(*set_args: str) -> str:
    result = _helm_clickhouse(*set_args)
    assert result.returncode != 0, f"expected helm template to fail, got:\n{result.stdout}"
    return result.stderr


def test_clickhouse_requires_password_or_existing_secret():
    """Default render without auth.password or auth.existingSecret fails closed."""
    assert "auth.password is required when auth.existingSecret is unset" in _render_clickhouse_error()
    docs = _render_clickhouse("auth.existingSecret=precreated-ch-auth")
    assert not [doc for doc in docs if doc["kind"] == "Secret"]


def test_clickhouse_single_node_default_renders():
    """Single-node default renders StatefulSet, config, secret, service, macros."""
    docs = _render_clickhouse("auth.password=test_password")
    kinds = [doc["kind"] for doc in docs]
    assert "StatefulSet" in kinds
    assert "Service" in kinds
    assert "ConfigMap" in kinds
    assert "Secret" in kinds

    # Default is single-node, 1 shard, 1 replica, no keeper
    sts_docs = [doc for doc in docs if doc["kind"] == "StatefulSet"]
    assert len(sts_docs) == 1
    shard0 = sts_docs[0]
    assert shard0["metadata"]["name"] == "test-release-clickhouse-shard-0"
    assert shard0["spec"]["replicas"] == 1
    vct = shard0["spec"]["volumeClaimTemplates"][0]["spec"]
    assert "storageClassName" not in vct
    assert vct["resources"]["requests"]["storage"] == "100Gi"

    # No keeper resources rendered
    assert not any("keeper" in doc["metadata"]["name"] for doc in docs)

    # ConfigMap contains dedup settings and system-log overrides
    main_cfg = next(
        doc
        for doc in docs
        if doc["kind"] == "ConfigMap" and doc["metadata"]["name"] == "test-release-clickhouse-config"
    )
    dedup_xml = main_cfg["data"]["dedup.xml"]
    assert "<replicated_deduplication_window>10000</replicated_deduplication_window>" in dedup_xml
    assert "<replicated_deduplication_window_seconds>86400</replicated_deduplication_window_seconds>" in dedup_xml

    syslogs_xml = main_cfg["data"]["system-logs.xml"]
    assert '<trace_log remove="1"/>' in syslogs_xml
    assert '<processors_profile_log remove="1"/>' in syslogs_xml

    # Macros ConfigMap contains single-node macros
    macro_cfg = next(doc for doc in docs if doc["kind"] == "ConfigMap" and "macros" in doc["metadata"]["name"])
    assert "<shard>01</shard>" in macro_cfg["data"]["macros.xml"]
    assert "<replica>01</replica>" in macro_cfg["data"]["macros.xml"]


def test_clickhouse_clustered_2x2_renders_shards_replicas_keeper_macros():
    """2-shard x 2-replica values file renders correct macros, Keeper, and dedup-window settings."""
    docs = _render_clickhouse("auth.password=test_password", "shards=2", "replicas=2", "keeper.enabled=true")

    # 2 ClickHouse StatefulSets (one per shard) each with 2 replicas
    ch_sts = [doc for doc in docs if doc["kind"] == "StatefulSet" and "shard" in doc["metadata"]["name"]]
    assert len(ch_sts) == 2
    for sts in ch_sts:
        assert sts["spec"]["replicas"] == 2

    # Keeper StatefulSet with 3 replicas and Headless Service
    keeper_sts = next(doc for doc in docs if doc["kind"] == "StatefulSet" and "keeper" in doc["metadata"]["name"])
    assert keeper_sts["spec"]["replicas"] == 3
    keeper_vct = keeper_sts["spec"]["volumeClaimTemplates"][0]["spec"]
    assert "storageClassName" not in keeper_vct
    assert keeper_vct["resources"]["requests"]["storage"] == "10Gi"
    assert any(doc["kind"] == "Service" and "keeper" in doc["metadata"]["name"] for doc in docs)

    # Remote servers config references shards and keeper
    main_cfg = next(
        doc
        for doc in docs
        if doc["kind"] == "ConfigMap" and doc["metadata"]["name"] == "test-release-clickhouse-config"
    )
    remote_xml = main_cfg["data"]["remote-servers.xml"]
    assert "test-release-clickhouse-shard-0-0" in remote_xml
    assert "test-release-clickhouse-shard-1-0" in remote_xml
    assert "test-release-clickhouse-keeper-0" in remote_xml

    # Dedup settings present
    dedup_xml = main_cfg["data"]["dedup.xml"]
    assert "<replicated_deduplication_window>10000</replicated_deduplication_window>" in dedup_xml
    assert "<replicated_deduplication_window_seconds>86400</replicated_deduplication_window_seconds>" in dedup_xml

    # Shard macros rendered for shard 0 and shard 1
    macro_cfgs = {
        doc["metadata"]["name"]: doc["data"]["macros.xml"]
        for doc in docs
        if doc["kind"] == "ConfigMap" and "macros" in doc["metadata"]["name"]
    }
    assert "<shard>01</shard>" in macro_cfgs["test-release-clickhouse-shard-0-macros"]
    assert "<shard>02</shard>" in macro_cfgs["test-release-clickhouse-shard-1-macros"]


def test_clickhouse_rejects_invalid_topology():
    """Bad shard/replica combinations fail at helm template."""
    assert "shards must be at least 1" in _render_clickhouse_error("auth.password=test_password", "shards=0")
    assert "replicas must be at least 1" in _render_clickhouse_error("auth.password=test_password", "replicas=0")
    assert "clustering requires keeper.enabled=true" in _render_clickhouse_error(
        "auth.password=test_password", "shards=2", "replicas=2", "keeper.enabled=false"
    )
    assert "keeper.replicas must be at least 1" in _render_clickhouse_error(
        "auth.password=test_password", "keeper.enabled=true", "keeper.replicas=0"
    )


# ── Merged from tests/chart/test_admin_gateway.py ──

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
GATEWAY = ROOT / "deploy/chart/admin-gateway"
APP = ROOT / "deploy/chart/fastly-log-analytics"
GATEWAY_VALUES = (
    "host=admin.example.com",
    "existingSecret=operator-gateway",
    "frontendUpstream=frontend:3000",
    "backendUpstream=backend:8000",
    "image=registry.example.com/fla-admin-gateway:test",
)


def render(chart, values, success=True):
    command = ["helm", "template", "test", str(chart)]
    for value in values:
        command += ["--set", value]
    result = subprocess.run(command, capture_output=True, text=True)
    assert (result.returncode == 0) == success, result.stderr
    return list(yaml.safe_load_all(result.stdout)) if success else result.stderr


def test_gateway_secret_reference_and_tls_only_service():
    docs = render(GATEWAY, GATEWAY_VALUES)
    config = next(doc for doc in docs if doc["kind"] == "ConfigMap")
    assert config["data"]["Caddyfile"] == (GATEWAY / "files/Caddyfile").read_text()
    assert "require_and_verify" in config["data"]["Caddyfile"]
    assert "tls internal" not in config["data"]["Caddyfile"]
    assert not any(doc["kind"] in {"Secret", "Ingress"} for doc in docs)
    service = next(doc for doc in docs if doc["kind"] == "Service")
    assert service["spec"]["type"] == "ClusterIP"
    assert service["spec"]["ports"] == [{"name": "mtls", "port": 8443, "targetPort": "mtls"}]
    pod = next(doc for doc in docs if doc["kind"] == "Deployment")["spec"]["template"]["spec"]
    assert pod["automountServiceAccountToken"] is False
    env = {item["name"]: item for item in pod["containers"][0]["env"]}
    assert env["ADMIN_GATEWAY_PORT"]["value"] == "8443"
    assert env["ADMIN_GATEWAY_SECRET"]["valueFrom"]["secretKeyRef"] == {
        "name": "operator-gateway",
        "key": "ADMIN_GATEWAY_SECRET",
    }
    secret = next(item["secret"] for item in pod["volumes"] if "secret" in item)
    assert {item["key"] for item in secret["items"]} == {"server.crt", "server.key", "client-ca.crt"}
    policy = next(doc for doc in docs if doc["kind"] == "NetworkPolicy")
    assert policy["spec"]["ingress"][0]["ports"][0]["port"] == 8443


@pytest.mark.parametrize(
    "host",
    ["", "*.example.com", "https://example.com", "example.com:443", "bad host", "bad..host", "-bad.host", "bad\nhost"],
)
def test_gateway_rejects_missing_or_wildcard_host(host):
    values = [value for value in GATEWAY_VALUES if not value.startswith("host=")]
    assert "explicit DNS hostname" in render(GATEWAY, [*values, f"host={host}"], success=False)


def test_gateway_requires_existing_secret_and_upstreams():
    for key, expected in (("existingSecret", "existingSecret"), ("frontendUpstream", "frontendUpstream")):
        values = [value for value in GATEWAY_VALUES if not value.startswith(f"{key}=")]
        assert expected in render(GATEWAY, values, success=False)


def test_internal_lb_reservation_has_no_gateway_or_secrets():
    docs = render(
        GATEWAY, ("gateway.enabled=false", "service.type=LoadBalancer", "service.internalProvider=gke-internal")
    )
    assert [doc["kind"] for doc in docs] == ["Service"]
    service = docs[0]
    assert service["metadata"]["annotations"]["cloud.google.com/load-balancer-type"] == "Internal"
    assert service["metadata"]["annotations"]["networking.gke.io/internal-load-balancer-allow-global-access"] == "true"
    assert service["spec"]["ports"][0]["port"] == 8443


def test_bare_loadbalancer_fails_closed():
    assert "internalProvider=gke-internal" in render(
        GATEWAY, (*GATEWAY_VALUES, "service.type=LoadBalancer"), success=False
    )


def test_internal_lb_active_gateway_keeps_original_tls_contract():
    docs = render(GATEWAY, (*GATEWAY_VALUES, "service.type=LoadBalancer", "service.internalProvider=gke-internal"))
    service = next(doc for doc in docs if doc["kind"] == "Service")
    assert service["metadata"]["annotations"]["cloud.google.com/load-balancer-type"] == "Internal"
    assert any(doc["kind"] == "Deployment" for doc in docs)


def test_app_default_is_unchanged_and_mtls_wires_both_containers():
    normal = render(APP, STANDARD)
    assert not any("public-gateway" in doc.get("metadata", {}).get("name", "") for doc in normal)
    docs = render(
        APP,
        (
            *STANDARD,
            "adminGateway.enabled=true",
            "adminGateway.existingSecret=operator-gateway",
            "adminGateway.host=admin.example.com",
            "adminGateway.image=registry.example.com/fla-admin-gateway:test",
        ),
    )
    for component in ("frontend", "backend"):
        deployment = next(
            doc for doc in docs if doc["kind"] == "Deployment" and doc["metadata"]["name"].endswith(f"-{component}")
        )
        env = {item["name"]: item for item in deployment["spec"]["template"]["spec"]["containers"][0]["env"]}
        assert env["ADMIN_GATEWAY_SECRET"]["valueFrom"]["secretKeyRef"]["name"] == "operator-gateway"
        assert env["ADMIN_GATEWAY_REQUIRED"]["value"] == "1"
        if component == "backend":
            assert env["ADMIN_GATEWAY_REQUIRED"]["value"] == "1"
            assert "admin.example.com" in env["LOCAL_HOSTS"]["value"].split(",")
    ingress = next(doc for doc in docs if doc["kind"] == "Ingress")
    for rule in ingress["spec"]["rules"]:
        assert all(path["backend"]["service"]["name"].endswith("-public-gateway") for path in rule["http"]["paths"])
    public = next(
        doc for doc in docs if doc["kind"] == "ConfigMap" and doc["metadata"]["name"].endswith("-public-gateway")
    )
    assert "request_header -X-Admin-Gateway-Token" in public["data"]["Caddyfile"]
    assert "-X-Remote-Analyst" not in public["data"]["Caddyfile"]


def _empty_dirs(docs):
    return [
        volume["emptyDir"]
        for doc in docs
        if doc and doc["kind"] == "Deployment"
        for volume in doc["spec"]["template"]["spec"].get("volumes", [])
        if "emptyDir" in volume
    ]


def test_every_emptydir_is_size_capped_for_admission_policies():
    gateway = _empty_dirs(render(GATEWAY, GATEWAY_VALUES))
    app = _empty_dirs(
        render(
            APP,
            (
                *STANDARD,
                "adminGateway.enabled=true",
                "adminGateway.existingSecret=operator-gateway",
                "adminGateway.host=admin.example.com",
                "adminGateway.image=registry.example.com/fla-admin-gateway:test",
            ),
        )
    )
    assert len(gateway) == 2
    assert all(volume.get("sizeLimit") for volume in gateway + app)
    assert all(volume["sizeLimit"] == "64Mi" for volume in gateway)


def test_gateway_image_override_renders_approved_mirror():
    image = "registry.example.com/mirror/library/caddy:2.11.4-alpine@sha256:" + "a" * 64
    docs = render(GATEWAY, (*GATEWAY_VALUES, f"image={image}"))
    deployment = next(doc for doc in docs if doc["kind"] == "Deployment")
    assert deployment["spec"]["template"]["spec"]["containers"][0]["image"] == image


def test_gateway_image_is_required_in_both_charts():
    # The upstream caddy image cannot exec with all capabilities dropped.
    without = tuple(value for value in GATEWAY_VALUES if not value.startswith("image="))
    render(GATEWAY, without, success=False)
    render(
        APP,
        (
            *STANDARD,
            "adminGateway.enabled=true",
            "adminGateway.existingSecret=operator-gateway",
            "adminGateway.host=admin.example.com",
        ),
        success=False,
    )
    assert render(GATEWAY, (*without, "gateway.enabled=false"))


def test_ci_publishes_gateway_image_alongside_app_images():
    ci_pipeline = (ROOT / "Jenkinsfile").read_text()
    assert "dockerFile: 'deploy/admin-gateway/Dockerfile'" in ci_pipeline
    assert "FLA_ADMIN_GATEWAY_IMAGE" in ci_pipeline
    # Built, commit-tagged and release-tagged on the same paths as the app images.
    assert ci_pipeline.count("containerName: adminGatewayImage") == 2
    dockerfile = (ROOT / "deploy/admin-gateway/Dockerfile").read_text()
    assert 'test -z "$(getcap /usr/bin/caddy)"' in dockerfile
    assert "USER 1000:1000" in dockerfile


def test_gateway_dockerfile_rewrites_caddy_binary_unconditionally():
    """Replace the binary rather than relying on capability-only layer changes."""
    dockerfile = (ROOT / "deploy/admin-gateway/Dockerfile").read_text()
    run = " ".join(
        line.strip().rstrip("\\") for line in dockerfile.split("RUN ", 1)[1].split("USER", 1)[0].splitlines()
    )
    assert "setcap" not in run
    assert 'getcap /usr/bin/caddy)" ]' not in run
    assert "cp /usr/bin/caddy /tmp/caddy" in run
    assert "chmod 0755 /tmp/caddy" in run
    assert "mv -f /tmp/caddy /usr/bin/caddy" in run
    assert run.index("cp /usr/bin/caddy") < run.index("mv -f /tmp/caddy")
    assert 'test -z "$(getcap /usr/bin/caddy)"' in run
