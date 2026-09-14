# Ingest load-test GKE runbook

Temporary infrastructure in `<gcp-project-id>` for testing ingest
throughput up to 50k RPS. Everything here is meant to be deleted after
testing — see **Teardown** at the bottom, and do it.

## Status / prerequisites

- Blocked on: `<gke-service-account>@<gcp-project-id>.iam.gserviceaccount.com`
  needs `roles/logging.logWriter`, `roles/monitoring.metricWriter`,
  `roles/monitoring.viewer`, `roles/stackdriver.resourceMetadata.writer`,
  `roles/artifactregistry.reader` (project Editor can't grant IAM roles;
  needs an Owner/IAM Admin on the project). See conversation history for the
  exact grant command.
- Already done: Artifact Registry repo `fla-loadtest` (us-central1),
  Fastly service `<fastly-service-id>` created with domain
  `<fastly-domain>` (backend not yet wired —
  needs the GKE Ingress IP, which doesn't exist until the cluster does).
- **Still needed before install**: the chart expects TWO images,
  `{image.repository}-backend:{tag}` and `{image.repository}-frontend:{tag}`
  (confirmed against local k8s validation — a single `backend:loadtest`
  image, which is what got pushed initially, resolves to the wrong name
  `backend-backend:loadtest` and the frontend pod has no image at all).
  Push both with a common repository prefix:

  ```bash
  docker buildx build --platform linux/amd64 -f backend/Dockerfile \
    -t us-central1-docker.pkg.dev/<gcp-project-id>/fla-loadtest/fla-backend:loadtest --push .
  docker buildx build --platform linux/amd64 -f frontend/Dockerfile \
    -t us-central1-docker.pkg.dev/<gcp-project-id>/fla-loadtest/fla-frontend:loadtest --push .
  ```

  Then `image.repository` in `values-loadtest.yaml` must be
  `us-central1-docker.pkg.dev/<gcp-project-id>/fla-loadtest/fla` (no
  `-backend`/`-frontend` suffix — the chart appends it).

## 1. Create the cluster (once IAM is granted)

```bash
gcloud container clusters create-auto fla-loadtest \
  --region=us-central1 \
  --service-account=<gke-service-account>@<gcp-project-id>.iam.gserviceaccount.com
gcloud container clusters get-credentials fla-loadtest --region=us-central1
```

## 2. Generate secrets (never commit these)

```bash
# hex, not base64: base64 can produce '/', '+', '=', and Helm's --set
# mini-language treats '=' as a key/value separator -- a base64 password
# threaded through --set silently corrupts the DSN (confirmed: it produced
# "failed to resolve host 'fla'", the username, instead of connecting to
# postgres -- Helm had truncated the value at the embedded '=').
PGPASS=$(openssl rand -hex 24)
kubectl create secret generic fla-loadtest-postgres --from-literal=password="$PGPASS"
kubectl create secret generic fla-loadtest-app-secret \
  --from-literal=CELERY_BROKER_URL="redis://valkey-master:6379/0" \
  --from-literal=METADATA_DSN="postgresql://fla:${PGPASS}@postgres:5432/ducklake"
```

## 3. Postgres + valkey

```bash
kubectl apply -f scripts/load_test/k8s/postgres-pvc.yaml -f scripts/load_test/k8s/postgres-deployment.yaml -f scripts/load_test/k8s/postgres-service.yaml
kubectl apply -f scripts/load_test/k8s/valkey-deployment.yaml -f scripts/load_test/k8s/valkey-service.yaml
kubectl wait --for=condition=available deployment/postgres deployment/valkey --timeout=180s
```

## 4. Install the chart

`values-loadtest.yaml`'s `config.ducklakeCatalog` has a placeholder
password — override it at install time so the real one never touches disk:

```bash
helm install fla-loadtest deploy/chart/fastly-log-analytics \
  -f scripts/load_test/k8s/values-loadtest.yaml \
  --set config.ducklakeCatalog="postgresql://fla:${PGPASS}@postgres:5432/ducklake"
```

Run schema init the same way `docker-compose`'s `metadata-schema-init`
does — check `scripts/setup_pg_schema.py` and run it as a one-shot
`kubectl run` or `kubectl exec` against a backend pod once one is up.

## 5. Admin access (tunnel, matching the GCE-VM SSH-tunnel pattern)

```bash
kubectl port-forward svc/fla-loadtest-fastly-log-analytics-backend 3001:80
```

Then browse `http://localhost:3001`. **Before relying on this for real admin
work**, verify the loopback-trust assumption documented in
`values-loadtest.yaml`: hit a debug/health endpoint through the tunnel and
confirm the backend sees the client as `127.0.0.1`, not some other address.
If it's not loopback, admin requests will get treated as remote/analyst —
narrowly widen `LOCAL_ADMIN_CIDRS` with the real observed address, don't
guess a broad range preemptively.

## 6. Public access

Once the Ingress has an IP (`kubectl get ingress`), wire the Fastly
service's backend to it:

```bash
FASTLY_API_TOKEN=... fastly service backend create \
  --service-id <fastly-service-id> --version 1 --autoclone \
  --name gke-origin --address <INGRESS_IP> --port 443 --use-ssl \
  --ssl-cert-hostname <fastly-domain> \
  --ssl-sni-hostname <fastly-domain>
fastly service version validate --service-id <fastly-service-id> --version latest
fastly service version activate --service-id <fastly-service-id> --version latest
```

Then `https://<fastly-domain>/` is the public,
analyst-path URL — masked/RBAC-gated like real production, not admin-trusted.

## 7. Run the load test

```bash
export FOS_KEY=$(python3 -c "import json; print(json.load(open('configs/<logging-service-id>.json'))['fos_access_key_id'])")
export FOS_SECRET=$(python3 -c "import json; print(json.load(open('configs/<logging-service-id>.json'))['fos_secret_access_key'])")
uv run python scripts/load_test/generate_synthetic_raw_logs.py \
  --bucket <fos-bucket-name> \
  --endpoint https://us-east-1.object.fastlystorage.app \
  --access-key-id "$FOS_KEY" --secret-access-key "$FOS_SECRET" \
  --target-rps 50000 --log-period-seconds 10 --shards 20 --duration-seconds 300
```

Capture during the run: per-worker throughput ceiling (`kubectl top pods`,
worker replica count from HPA/KEDA), Postgres commit rate (ledger/DuckLake
snapshot growth), and FOS LIST+DELETE call volume (`usage_log` — GET should
show near-zero direct calls since it's CDN-fronted).

## Teardown

```bash
helm uninstall fla-loadtest
kubectl delete -f scripts/load_test/k8s/postgres-pvc.yaml -f scripts/load_test/k8s/postgres-deployment.yaml -f scripts/load_test/k8s/postgres-service.yaml -f scripts/load_test/k8s/valkey-deployment.yaml -f scripts/load_test/k8s/valkey-service.yaml
gcloud container clusters delete fla-loadtest --region=us-central1 --quiet
gcloud artifacts repositories delete fla-loadtest --location=us-central1 --quiet
gcloud iam service-accounts delete <gke-service-account>@<gcp-project-id>.iam.gserviceaccount.com --quiet
fastly service delete --service-id <fastly-service-id> --force
```
