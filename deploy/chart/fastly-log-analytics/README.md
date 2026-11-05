# fastly-log-analytics (Helm chart)

## Architecture

Optional admin client-mTLS is configured through `adminGateway.enabled`,
`adminGateway.existingSecret`, `adminGateway.host` and the required
`adminGateway.image` (built from `deploy/admin-gateway/Dockerfile`, used by
the public header-sanitizing proxy); its separate companion
release and certificate lifecycle are documented in
[admin-gateway/README.md](../admin-gateway/README.md).
The public Ingress uses a credential-stripping proxy when this option is on.

The chart deploys the unified Fastly Log Analytics stack on Kubernetes:
- **Backend** (`replicas: 1`) — FastAPI serving tier reading analytical queries from ClickHouse and operational metadata from PostgreSQL 16.
- **Worker** (`workers.replicaCount`, default `1`) — Stateless ingest worker (`python -m backend.high_scale.worker`) configured with the lightweight pod profile (`1 vCPU / 2 GiB`, `HIGH_SCALE_WORKER_THREADS=1`, `HIGH_SCALE_MAX_BATCH_BYTES=6291456`, `HIGH_SCALE_MAX_BATCH_OBJECTS=10`).
- **Frontend** (`replicaCount`, default `1`) — Next.js App Router dashboard.

## Prerequisites

The chart renders at `helm template` time with default values and a ReadWriteOnce StorageClass (`persistence.storageClassName`, default empty, i.e. the cluster default):

```sh
helm template fla ./deploy/chart/fastly-log-analytics
```

At runtime, the deployment requires **PostgreSQL 16** (`secrets.metadataDsn` or `secrets.existingSecret`, per [ADR-22](../../../docs/adr/22-postgres-only-metadata.md)) and **ClickHouse** (`config.clickhouseUrl` / `config.clickhouseHost`).

```sh
helm install fla ./deploy/chart/fastly-log-analytics \
  --set config.clickhouseUrl=http://clickhouse:8123 \
  --set secrets.existingSecret=fla-connections
```

where `fla-connections` is a Secret you created first:

```sh
kubectl create secret generic fla-connections \
  --from-literal=METADATA_DSN=postgresql://fla:PASSWORD@postgres:5432/fla \
  --from-literal=CLICKHOUSE_PASSWORD=PASSWORD \
  --from-literal=CELERY_BROKER_URL=redis://valkey-master:6379/0
```

`secrets.metadataDsn` / `secrets.clickhousePassword` / `secrets.celeryBrokerUrl` are the convenience alternative for a test cluster — they land in the release manifest, readable to anyone with helm access, so prefer `existingSecret` for production.

Run `scripts/setup_pg_schema.py` against the metadata database before first boot.

## Optional high-scale application plane subchart

The chart also contains an opt-in `highScale` application subchart for multi-workload policies (PodDisruptionBudget, NetworkPolicy, anti-affinity, and dedicated ServiceAccount):

```sh
helm install fla ./deploy/chart/fastly-log-analytics \
  --set highScale.enabled=true \
  --set highScale.secret.existingName=fla-high-scale-connections
```

## Persistence

`persistence.enabled: false` swaps the PVC for an `emptyDir`, which loses
`/app/configs` — the service registry — on every pod restart, so the install
forgets every configured service. Only use it for a throwaway install.

## Admin access

The GCE/Compose deploy classifies admin vs. analyst by loopback: Caddy talks
to uvicorn over `127.0.0.1`, so only that peer (or an SSH tunnel terminating
there) is trusted (see `backend/utils/remote_access.py`). Nothing in a k8s
cluster ever arrives over real loopback, so this chart needs the same
guarantee re-established two different ways depending on how you connect:

- **`kubectl port-forward svc/<release>-backend <port>:8000` direct to the
  backend** — no config needed. `kubectl port-forward` tunnels into the
  target pod's own network namespace, so the connection looks like
  `127.0.0.1` to the app: genuine loopback, admin by default. Use this for
  `/docs`, `/api/admin/*`, or any direct API call.
- **`kubectl port-forward svc/<release>-frontend <port>:3000` to browse the
  UI** — the browser's hop to the frontend is loopback-equivalent the same
  way, but the frontend's own SSR then calls the backend over the *real*
  in-cluster pod network (frontend pod -> backend Service), which is not
  loopback. Without `config.localAdminCidrs` set to the cluster's actual pod
  CIDR (`kubectl get nodes -o jsonpath='{.items[0].spec.podCIDR}'`), that hop
  is classified remote/analyst — PII-masked, blocked from admin endpoints —
  even though nothing is publicly exposed. Set it and the UI gets full admin
  access through the same port-forward.

`config.localAdminCidrs` is refused outright if it's wider than a `/16` or
not a private range (see the guard in `remote_access.py`) — scope it to the
cluster's actual pod CIDR, never something broader.
