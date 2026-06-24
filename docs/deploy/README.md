# Deployment runbooks

The application runs on **any Linux VM with Docker**. Storage stays on **Fastly Object Storage** (S3-compatible) regardless of host platform. Only the host-provisioning + firewall + secrets-source steps differ per platform; the Caddy config, docker compose file, `restart.sh` body, and post-deploy curl commands are identical across all four runbooks below.

## Platform runbooks

- [aws_ec2.md](aws_ec2.md) — Amazon Linux 2023 + Docker, IMDSv2 session-token note, SG rules, EBS volume mount, optional systemd unit.
- [gce.md](gce.md) — Debian/Ubuntu + Docker, persistent-disk mount, formalized `restart.sh` flow.
- [azure_vm.md](azure_vm.md) — Ubuntu 22.04 + Docker, NSG rules, managed-identity secrets pattern with Key Vault.
- [generic_linux.md](generic_linux.md) — Linode / DigitalOcean / Hetzner / bare metal, env-file secrets pattern, provider-specific firewall gotchas.

Each runbook covers the same seven sections so a reader switching platforms can diff them side-by-side: host provisioning, Docker install, volume mount, Caddy/SSL, first deploy + `restart.sh`, secrets management, and post-deploy verification.

## What stays the same across platforms

- The `restart.sh` script (`git pull` + `docker compose --build` + healthcheck).
- The docker compose file and Caddy configuration.
- Fastly Object Storage access (boto3 + Fastly's S3-compatible endpoint).
- The admin SSH-port-forward auth flow — a network-layer primitive, not platform-specific.

## What varies

- **Cloud metadata service.** All three major clouds expose metadata at `169.254.169.254`. AWS requires IMDSv2 session tokens by default; GCE and Azure do not. The SSRF guard in `backend/models/lake.py` blocks that IP in URL validation; nothing in the app reads from the metadata service itself.
- **Firewall.** AWS security groups, GCE firewall rules, Azure NSGs — same goal, different UX.
- **Secrets source.** AWS Secrets Manager / GCE Secret Manager / Azure Key Vault / env-file on generic Linux. The runbooks document the recommended pattern for each.

## Out of scope

- Non-Fastly object storage (GCS, S3, ADLS). Storage stays Fastly per the v2.0 storage-model decision (see [adr/01-storage-model.md](../adr/01-storage-model.md)).
- Multi-region deploy. Single-region per the v2.0 plan.
- Kubernetes / Nomad / other orchestrators. Single-host docker compose stays the deploy model.
