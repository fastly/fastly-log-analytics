# Cloud Portability Audit — Phase 0

**Goal:** confirm the codebase runs on any Linux VM with Docker, not just Google Compute Engine. Storage stays Fastly Object Storage (per the v2.0 decision); only the host VM platform varies.

## Method

Greps over `backend/` for `GCE`, `gcloud`, `google-cloud`, `gcsfs`, etc., plus targeted reads of every match.

## Findings

### Zero cloud-specific code

No imports of `google-cloud-*`, `gcsfs`, `boto3-specific-to-aws-only`, or `azure-storage-*` packages in `pyproject.toml`. Boto3 is used against Fastly's S3-compatible API (`endpoint_url=<fastly-fos-endpoint>`), which works identically on any cloud VM.

No shell-outs to `gcloud`, `aws`, or `az` CLIs anywhere in backend code or scripts.

### 7 GCE references found in code comments (pre-cleanup)

The Phase 0 grep found 7 GCE references — all in comments/docstrings, zero in executable code:

| File:line | Pre-cleanup wording | Post-cleanup wording | Reason |
|---|---|---|---|
| `backend/state_sync.py:143` | "GCE backend can fetch the same matrix" | "the prod VM backend can fetch the same matrix" | Generic VM language |
| `backend/state_sync.py:272` | "(GCE metadata, peer VMs, link-local addresses)" | "(cloud metadata at 169.254.169.254 on AWS/GCE/Azure, ...)" | All three clouds share the link-local IP — listing all three is more accurate |
| `backend/provision/session_scoring_orchestrator.py:614` | "(and the GCE prod backend)" | "(and the prod VM backend)" | Generic VM language |
| `backend/core/iceberg.py:3652` | "spamming the GCE backend log" | "spamming the prod VM backend log" | Generic VM language |
| `backend/utils/remote_access.py:128` | "``169.254.169.254`` (GCE metadata)" | "``169.254.169.254`` (cloud metadata service — same IP on AWS, GCE, Azure)" | Clarifies multi-cloud applicability |
| `backend/models/lake.py:14` | "SSRF probe of the GCE metadata service" | "SSRF probe of the cloud metadata service (same link-local IP on AWS, GCE, and Azure)" | Clarifies multi-cloud applicability |

The post-cleanup wording for AWS/GCE/Azure-shared concepts intentionally lists all three to make multi-cloud applicability obvious to the next reader.

### Other "GCE" mentions outside `.py` files

None requiring code changes. The plan's `gce-deploy-rebuild` memory entry will be updated to "vm-deploy-rebuild" later (the `~/restart.sh` flow is cloud-agnostic — it's `git pull` + `docker compose --build` + healthcheck).

## Conclusion

**The codebase is already VM-agnostic.** No code changes were required for cloud portability beyond comment cleanup. Phase 10.8 will add per-platform deploy runbooks (`docs/deploy/aws_ec2.md`, `gce.md`, `azure_vm.md`, `generic_linux.md`).

### What this means for Phase 10.8 runbooks

The runbooks are pure documentation — they all reference the same code paths. The cloud-specific knowledge they capture:

- **AWS EC2**: instance metadata service v2 (IMDSv2) is the default and requires a session token; document the change vs IMDSv1; security group setup for ports 80/443 + the SSH port-forward.
- **GCE**: documented today implicitly; formalize the existing flow (`gce-deploy-rebuild` memory).
- **Azure VM**: NSG (network security group) rules for ports 80/443 + SSH; document the Azure-specific managed identity option if any user wants it (vs hardcoded service account).
- **Generic Linux** (bare metal, Linode, DigitalOcean, etc.): no metadata service guarantees; users supply credentials via env vars; everything else is the same as the cloud paths.

### What stays unchanged

- The `~/restart.sh` script (`git pull` + `docker compose --build` + healthcheck) works on every platform.
- Docker compose file is cloud-agnostic.
- Caddy config is cloud-agnostic.
- Fastly Object Storage access is cloud-agnostic (boto3 + Fastly S3-compatible endpoint).
- Admin SSH-port-forward auth works on any cloud (it's a network-layer primitive, not platform-specific).

## Out of scope

- Non-Fastly object storage backends (GCS, S3, ADLS). Storage stays Fastly per the v2.0 decision.
- Multi-region deploy. Single-region per the plan's "out of scope" list.
- Kubernetes / Nomad / other orchestrators. Single-host docker compose stays the deploy model.

## Phase 10.8 deliverables — per-platform runbooks

The four runbooks shipped under `docs/deploy/`:

- [`docs/deploy/aws_ec2.md`](../docs/deploy/aws_ec2.md) — Amazon Linux 2023 + Docker, IMDSv2 session-token note, SG rules, EBS at `/mnt/app-data`, optional systemd unit.
- [`docs/deploy/gce.md`](../docs/deploy/gce.md) — Debian/Ubuntu + Docker, formalized `~/restart.sh` flow, persistent disk at `/mnt/app-data`.
- [`docs/deploy/azure_vm.md`](../docs/deploy/azure_vm.md) — Ubuntu 22.04 + Docker, NSG rules, managed-identity-preferred secrets pattern with Key Vault.
- [`docs/deploy/generic_linux.md`](../docs/deploy/generic_linux.md) — Linode / DigitalOcean / Hetzner / bare metal, env-file secrets pattern, provider-specific firewall gotchas.

Each runbook covers the same seven sections (host provisioning, Docker install, volume mount, Caddy/SSL, first-deploy + `restart.sh`, secrets management, post-deploy verification) so a reader switching platforms can diff them side-by-side. The cloud-agnostic pieces (Caddy config, docker compose file, `restart.sh` body, post-deploy curl commands) are identical across all four — only the host-provisioning, firewall, and secrets-source steps differ.
