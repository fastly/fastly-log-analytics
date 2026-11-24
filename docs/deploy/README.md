# Deployment runbooks

The application runs on **any Linux VM with Docker**. Storage stays on **Fastly Object Storage** (S3-compatible) regardless of host platform. Only the host-provisioning + firewall + secrets-source steps differ per platform; the Caddy config, docker compose files, `restart.sh` body, and post-deploy curl commands are identical across all four runbooks below.

## Platform runbooks

- [Optional private telemetry](observability.md) — existing SDK directly
  to Prometheus and Tempo for Docker Compose and the existing Helm receivers;
  opt-in private listeners/storage, delivery proofs and a private Metrics UI.

- [Optional admin mTLS gateway](../../deploy/chart/admin-gateway/README.md) —
  shared Docker Compose/Kubernetes gateway, operator-owned certificates,
  explicit client installation, expiry and emergency CA rotation. The
  gateway is opt-in; retain host-management SSH.

- [aws_ec2.md](aws_ec2.md) — Amazon Linux 2023 + Docker, IMDSv2 session-token note, SG rules, EBS volume mount, optional systemd unit.
- [gce.md](gce.md) — Debian/Ubuntu + Docker, persistent-disk mount, `restart.sh` flow.
- [azure_vm.md](azure_vm.md) — Ubuntu 22.04 + Docker, NSG rules, managed-identity secrets pattern with Key Vault.
- [generic_linux.md](generic_linux.md) — Linode / DigitalOcean / Hetzner / bare metal, env-file secrets pattern, provider-specific firewall gotchas.

Each runbook covers the same seven sections so a reader switching platforms can diff them side-by-side: host provisioning, Docker install, volume mount, Caddy/SSL, first deploy + `restart.sh`, secrets management, and post-deploy verification.

## What stays the same across platforms

- The `restart.sh` pattern: a small script you keep on the VM (`git pull` + `docker compose build` + container swap + healthcheck); the reference body is in [gce.md](gce.md).
- The docker compose files (`docker-compose.yml` + the `docker-compose.prod.yml` production overlay; never a plain `docker compose up` in production) and Caddy configuration.
- PostgreSQL 16 and ClickHouse 24.8 (provided by the compose stack) for operational metadata, the ingest ledger, and analytical storage, plus the stateless ingest worker (`python -m backend.high_scale.worker`).
- Fastly Object Storage access (boto3 + Fastly's S3-compatible endpoint).
- The admin SSH-port-forward auth flow — a network-layer primitive, not platform-specific.

## What varies

- **Cloud metadata service.** All three major clouds expose metadata at `169.254.169.254`. AWS requires IMDSv2 session tokens by default; GCE and Azure do not. Nothing in the app reads from the metadata service itself.
- **Firewall.** AWS security groups, GCE firewall rules, Azure NSGs — same goal, different UX.
- **Secrets source.** AWS Secrets Manager / GCE Secret Manager / Azure Key Vault / env-file on generic Linux. The runbooks document the recommended pattern for each.

## Out of scope

- Non-Fastly object storage (GCS, S3, ADLS). Storage stays on Fastly Object Storage.
- Multi-region deploy. Single-region.
- Nomad / other orchestrators. The VM guides here use single-host docker compose; Kubernetes deployments are covered by the Helm charts under `deploy/chart/`.

## Cloud Serverless (Fastly Compute + Fastly Object Storage)

For zero-origin serverless deployments running entirely on Fastly Compute (`compute/cloud/`), Fastly VCL, Fastly Object Storage (FOS), and the Browser DuckDB-Wasm engine (`frontend/lib/cloud-engine/`):

- [Architecture & Recommendation](../cloud/recommendation.md) — pipeline design ("Option F"), VCL `/_tick` clock, and Compute coordinator/worker fan-out.
- [Cost Methodology](../cloud/cost-methodology.md) — Fastly Compute, VCL, and FOS pricing formulas and rollup economics.
- [Security & Route Access](../cloud/security-route-access.md) — passkey authentication, front-service header validation, and route-level access control.
- [Autonomous Cron Jobs](../cloud/crons/README.md) — bounded ingest conversion, compaction, rollup construction, and retention stages.
- Build & deploy via root `Makefile`: `make cloud-build` and `make cloud-deploy NAME=<install>` (implemented in `compute/cloud/infra/provision.py` and `compute/cloud/tools/release.py`).
