# Optional private deployment telemetry

This extends the existing application SDK → Prometheus/Tempo path for **Standard
Compose**, locally and on a single Linux VM. It adds no Collector, Grafana,
exporters, packages, application endpoint or edge traffic. Deployments that omit
the overlays keep their existing behavior, including production's disabled
exporter default. The existing experimental Helm chart also has a separate
minimal receiver opt-in, described below.

The existing `docker-compose.observability.yml` remains the **multipod** overlay.
It still includes Grafana and infrastructure exporters on `app-network`; do not
merge it into Standard or host-network production.

## Configure an opt-in

Keep these settings in the operator-owned, untracked `.env`, mode `0600`:

```dotenv
OTEL_EXPORTER=otlp
OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=http://127.0.0.1:9090/api/v1/otlp/v1/metrics
OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://127.0.0.1:4318/v1/traces
```

Both **full signal paths** are required by the existing Python exporters.
The public overlays only pass settings through; they never enable export by
default or invent a destination for an unset endpoint.

Append the appropriate files to the deployment's existing Compose file list:

| Deployment | Append in this order |
|---|---|
| Local Standard | `docker-compose.observability.standard.yml` |
| GCE/Linux Standard | `docker-compose.observability.standard.yml`, then `docker-compose.observability.standard.prod.yml` |

Retain the original base/prod files and every existing mTLS/public-HTTPS overlay.
In particular, do not replace the current `COMPOSE_FILE` with a shortened list
that drops administrator authentication. Validate the complete configuration
with `docker compose config --quiet` before using the authorized deployment
procedure. For a public-only checkout without `COMPOSE_FILE`, equivalent static
checks are:

```sh
docker compose --env-file /dev/null \
  -f docker-compose.yml -f docker-compose.observability.standard.yml config --quiet
docker compose --env-file /dev/null \
  -f docker-compose.yml -f docker-compose.prod.yml \
  -f docker-compose.observability.standard.yml \
  -f docker-compose.observability.standard.prod.yml config --quiet
```

Activation/recreation requires a separately authorized deployment. A restart
script that selects only `backend frontend` must also reconcile the optional
`prometheus tempo` services; merely adding them to `COMPOSE_FILE` does not start
them. Local services share the backend network namespace and must be recreated
together when that namespace is replaced.

### Operator-local canonical runner

`scripts/dev/deploy_test_all.sh` is private and gitignored, not a public runtime
dependency. Its explicit `--with-observability` flag selects all three deployment
targets. `LOCAL_STANDARD_OBSERVABILITY=1` or
`GCE_STANDARD_OBSERVABILITY=1` selects one Standard target;
`HIGH_SCALE_OBSERVABILITY=1` selects High-Scale. All default to `0`.

The opt-in reconciles even an already-current application commit. Local config
mounts come from the runner's immutable committed export, not working-tree
edits. GCE updates only its private `.env`, retaining mTLS overlays and rejecting
conflicting external OTLP destinations; those VM settings persist across later
restarts. Local selection is per invocation unless saved in private operator
configuration. Keep the flag on subsequent local deployments to retain the
overlay.

The option is rejected with resume, reset, watch and infrastructure-bootstrap
modes, or when the selected target is skipped. Existing verify-correlated-only
dispatch is unchanged. This is not a deployment authorization, a verification-only
mode, or a replacement for the runner's existing pushed-HEAD and mTLS gates.
`--no-traffic` remains independent; the telemetry probe itself sends no edge
traffic.

### Metrics UI

**Prometheus has a built-in query UI. Tempo is API-only.** This option does not
start or test Grafana.

After delivery verification, the canonical runner opens private UI access and
uses the existing Playwright Chromium dependency to verify the actual Prometheus
query editor and Execute control. A listening port or successful API response
does not qualify. Only then does each selected environment card in the generated
HTML report receive its own **Metrics UI** link.

The links use separate dynamically allocated `127.0.0.1` ports:
Local Standard uses a bounded HTTP relay through backend-container execution;
GCE uses a runner-owned SSH local forward to VM loopback `9090`;
High-Scale uses a runner-owned `kubectl port-forward --address=127.0.0.1` to
the existing Prometheus ClusterIP service. No receiver port is published, no
public proxy/authentication route is added, and port `3000` stays untouched.
The local relay strips browser cookies/authorization rather than passing
application credentials to Prometheus.

Open the environment's link from the same workstation that ran the canonical
runner. Successful runs retain these owned access processes for **at most one
hour**, including bounded runs. Failed runs remove links and stop their access
processes. The report labels the expiry; an old static report cannot resurrect
an expired forward. To close access early, use its private evidence directory:

```sh
python -m scripts.lib.observability_ui stop-directory <report-directory>/metrics-ui.<run-suffix>
```

Do not start ad-hoc replacement forwards. A subsequent authorized canonical run
creates and browser-verifies fresh links. SSH/kubectl credentials, application
certificates, and private access-state files stay operator-local; they never
appear in link URLs or tracked files.

## Private listeners and resource envelope

Local receivers join the backend loopback and publish **no ports** on the host.
Production receivers use host networking but bind only `127.0.0.1`:

| Component | Listener | Storage / retention |
|---|---|---|
| Prometheus | `9090`, query API and OTLP metrics | `standard-prometheus-data`, 24 hours or 512 MB of retained TSDB blocks |
| Tempo | `4318`, OTLP HTTP; `3200`, query API | `standard-tempo-data`, 24-hour trace retention, persisted WAL and blocks |

Tempo's internal gRPC/memberlist listeners also bind loopback. No Caddy routes,
firewall openings, Grafana port `3000`, remote-write receiver or public
unauthenticated telemetry endpoint is added. Query through backend-container
execution or an operator-controlled SSH tunnel on the VM. These APIs have no
application RBAC of their own: loopback and host/Docker administration are the
access boundary. Do not expose them to analysts or public interfaces.

Each receiver has a 0.5 CPU / 512 MiB memory cap, no swap allowance above that
memory cap, a 384 MiB Go heap target, and rotated logs (3 × 10 MB). Existing
backend memory/pool/proxy-trust settings stay untouched. Budget up to **1 GiB
additional memory and one CPU** before enabling on a small VM. Saturation can
reject telemetry or restart a receiver; it must not loosen the application's
existing resource limits.

Named volumes survive container recreation, not host or disk loss. Retention is
not a filesystem quota: Prometheus head/WAL and Tempo WAL/compaction overhead
need additional disk space; Tempo has no hard volume byte ceiling here. Monitor
free space and stop collection before filling the application disk. These are
bounded, short-retention diagnostics, not an off-host durable telemetry archive.

## Verify actual delivery without the private runner

The backend runtime image includes the public helper. Run **both** commands
inside that container so they use its real deployed SDK/environment and its
private loopback query origins:

```sh
PROBE_ID="$(python3 -c 'from uuid import uuid4; print(uuid4())')"
docker compose exec -T backend python -m scripts.dev.verify_observability emit \
  --probe-id "$PROBE_ID" --output "/tmp/$PROBE_ID-receipt.json"
docker compose exec -T backend python -m scripts.dev.verify_observability verify \
  --receipt "/tmp/$PROBE_ID-receipt.json" \
  --prometheus-url http://127.0.0.1:9090 --tempo-url http://127.0.0.1:3200 \
  --output "/tmp/$PROBE_ID-verified.json" --timeout-seconds 90
```

Use the same Compose selection/project as the deployment. Files must not already
exist. The helper creates receipt/result files mode `0600`; copy them only into
an owner-only evidence directory, keep them untracked, and remove container
copies when finished. It refuses stale receipts and propagates verification
failure. A process exit or healthy receiver alone is **not** delivery evidence.

The diagnostic emits one `app.observability.probe` span with an exact UUID
`fla.probe.id` and one fixed counter. Verification requires the exact trace/span,
probe nonce and SDK resource identity plus a fresh
`app_observability_probe_total=1` series with the standard OTLP aliases
`job=fastly-log-analytics` and `instance=<service.instance.id>`. It does not add a
new per-probe metric label. No analytics query or application/edge call is needed.
Query clients validate TLS for HTTPS, ignore ambient proxy settings, refuse
redirects, and do not write URLs or credentials into evidence.

The canonical runner executes these same public commands in each backend and
both existing High-Scale worker containers, copies private receipts/results, and
fails its overall result if delivery is unverified. Those diagnostics prove
**container SDK pipeline delivery**, not instrumentation in already-running
worker processes.

The runner separately sends **one normal `/api/health` request per selected
environment**, through the existing exact-origin mTLS/local transport, with a
chosen incoming `traceparent`. It requires HTTP 200, then calls the public
`verify-request` command with a fresh owner-only receipt containing exactly
`trace_id`, `started_at`, and `path`. Verification requires the corresponding
fresh SERVER span and a fresh HTTP-duration count for the **same running
application instance**, not the extra diagnostic SDK process:

```sh
python -m scripts.dev.verify_observability verify-request \
  --receipt <owner-only-request-receipt.json> \
  --prometheus-url http://127.0.0.1:9090 --tempo-url http://127.0.0.1:3200 \
  --output <new-owner-only-result.json> --timeout-seconds 90
```

Run verification in the backend container, copying in the receipt securely.
For High-Scale use the private service origins `http://prometheus:9090` and
`http://tempo:3200`. These are helper call paths, not authorization to bypass
the canonical deployment procedure. Normal HTTP proof covers that request,
not all endpoints, worker instrumentation, historical completeness, page
performance or off-host durability. No edge beacons, ingestion traffic,
analytics query or correlated/load probe is needed.

## Disable or troubleshoot

On failure, inspect receiver and backend exporter logs before emitting another
fresh probe; expired or previously used receipts are not reusable. Check both
explicit OTLP endpoints, listener conflicts, storage permissions/free space and
receiver resource pressure. Do not substitute a health check for the delivery
test or enable console export.

To disable, restore the previous private exporter settings (normally `none`),
remove only these two optional overlays, and reconcile the affected containers
through the authorized deployment procedure. Stop/remove only the optional
receiver containers; retain their named volumes unless deletion is explicitly
intended. Do not use `down -v` against the application's Compose project.

## Existing High-Scale Helm receivers

The legacy `observability.enabled` bundle remains unchanged: Grafana, exporters,
ephemeral receiver storage and its existing configuration. Leave it off for the
minimal option. Instead select `global.privateObservability.enabled=true` in the
existing chart. The two options are mutually exclusive. This reuses the same
receiver templates/images and adds no Collector, new Kubernetes platform,
Grafana dependency, exporter or public telemetry ingress.

The minimal option configures the backend, Celery worker and enabled executable
High-Scale subchart workloads with explicit direct-OTLP signal paths. It rejects
conflicting explicit exporter/endpoints. Receiver pods use the existing default
500m CPU / 512 MiB limits, a 384 MiB heap target, non-root/read-only containers,
24-hour retention, and separate ReadWriteOnce PVCs (Prometheus `1Gi`, Tempo
`2Gi`). Storage sizes/class are operator values under
`global.privateObservability`; a working default or explicit StorageClass is
required. Prometheus retains at most 512 MB of TSDB blocks; PVC capacity still
needs head/WAL overhead. PVCs are not off-host durability guarantees.

Services remain ClusterIP. NetworkPolicies admit receiver/query traffic only
from namespace pods and deny receiver egress. The High-Scale subchart adds only
receiver destinations to its existing egress policy; it does not relax other
workload boundaries. Enforcement requires the cluster's NetworkPolicy-capable
CNI. Existing operator policies must permit application delivery; failed
delivery is not bypassed by weakening network controls.

The private canonical runner renders/applies **only these receiver templates**,
with explicit resource-ownership collision checks. It does not upgrade or
replace the application Helm release. It patches OTLP env on the existing
`backend`, `high-scale-worker` and `worker` containers, preserving other
settings, and waits for the canonical rollouts before verification. Independent
receiver resources with the same names cause a hard stop, not silent adoption.
Chart SDK configuration and successful child-process probes cannot manufacture
missing worker entrypoints or prove running-worker instrumentation.
