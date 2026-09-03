# fastly-log-analytics (Helm chart)

**EXPERIMENTAL.** This chart is not the supported deployment path — that is the
single-VM `docker-compose.prod.yml` at the repo root. The chart exists to
exercise the multi-pod Celery/valkey ingest split on Kubernetes.

## Two topologies

The chart renders one of two shapes, selected by `config.ingestMode`.

| | `sync` (default) | `celery` |
|---|---|---|
| Ingest | in-process APScheduler on the backend pod | Celery worker fleet + RedBeat |
| Pods | backend, frontend | backend, frontend, worker, beat |
| Postgres | not used | **required** (DuckLake catalog + metadata) |
| valkey/redis | not used | **required** (broker, RedBeat, SSE backplane) |
| Scales ingest | no | yes (`workers.replicaCount`, or KEDA via `autoscaling.enabled`) |

Neither mode scales the **serving** tier. The backend Deployment is pinned to
one replica in the template and has no HPA in either mode, because the
per-service `.duckdb` file is process-exclusive — see
[ADR-18](../../../docs/adr/18-serving-tier-single-pod.md). `replicaCount`
applies to the stateless frontend only.

## Prerequisites

`sync` mode needs only a cluster with a ReadWriteOnce StorageClass. A bare
`helm install` with no `--set` flags works:

```sh
helm install fla ./deploy/chart/fastly-log-analytics
```

`celery` mode needs **Postgres and valkey/redis, which this chart does not
ship.** `Chart.yaml` declares no subchart dependencies on purpose: vendoring
them would make `helm lint`/`helm template` (and therefore `make
deploy-validate`) depend on `helm dependency update` having network access.
Bring your own — a `bitnami/postgresql` and `bitnami/valkey` release in the
same namespace is enough for a test cluster — then:

```sh
helm install fla ./deploy/chart/fastly-log-analytics \
  --set config.ingestMode=celery \
  --set config.schedulerMode=external \
  --set config.sseBackplane=valkey \
  --set config.ducklakeCatalog=postgresql://fla:PASSWORD@postgres:5432/ducklake \
  --set secrets.existingSecret=fla-connections \
  --set broker.host=valkey-master
```

where `fla-connections` is a Secret you created first:

```sh
kubectl create secret generic fla-connections \
  --from-literal=CELERY_BROKER_URL=redis://valkey-master:6379/0 \
  --from-literal=METADATA_DSN=postgresql://fla:PASSWORD@postgres:5432/ducklake
```

`secrets.celeryBrokerUrl` / `secrets.metadataDsn` are the convenience
alternative for a test cluster — they land in the release manifest, readable
to anyone with helm access, so prefer `existingSecret` for anything real.

The two DSNs may point at the same Postgres database; every DuckLake table is
`ducklake_`-prefixed and does not collide with the metadata schema
([ADR-15](../../../docs/adr/15-multi-writer-topology.md)). Run
`scripts/setup_pg_schema.py` against the metadata database before first boot.

## Misconfiguration is a template-time error

`backend/config.py::validate_ingest_mode()` refuses to boot a backend or
worker whose `INGEST_MODE=celery` lacks a Postgres `DUCKLAKE_CATALOG` or
`METADATA_DSN`. `templates/validate.yaml` reproduces those conditions at
render time, so `helm install`/`helm template` fails with a message naming the
value to set instead of deploying backend, worker and beat pods that all
CrashLoop. It also rejects the combinations that fail *silently* at runtime:
an unrecognised `ingestMode` (the backend would quietly fall back to `sync`),
`schedulerMode=external` outside celery mode (RedBeat-routed jobs with no
worker fleet to consume them), and `sseBackplane=valkey` with no broker URL.

The one case it cannot check is an `existingSecret` missing a key — the chart
cannot read a Secret it did not create. In celery mode both `secretKeyRef`s
are non-optional, so that surfaces as the pod failing to start with
`couldn't find key CELERY_BROKER_URL in Secret`, not as a boot-gate
CrashLoop.

The invariants above are pinned by `tests/chart/test_helm.py` (in the pytest
suite); `make deploy-validate` additionally runs `helm lint` and a
default-values `helm template`.

## Persistence

`persistence.enabled: false` swaps the PVC for an `emptyDir`, which loses
`/app/configs` — the service registry — on every pod restart, so the install
forgets every configured service. Only use it for a throwaway install.

The PVC is ReadWriteOnce, so every pod mounting it is pinned to one node. That
includes the worker pods in celery mode: scaling workers across nodes needs
shared storage for `/app/cache` (and is subject to the pod-local-output
caveats in ADR-18).
