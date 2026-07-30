# Deploy to Azure VM

This runbook covers running the stack on an Ubuntu 22.04 Azure Linux VM with
Docker + docker compose. The backend image is the same as every other
platform; only the host-provisioning steps differ.

## 1. Host provisioning

- **Image**: `Canonical:0001-com-ubuntu-server-jammy:22_04-lts-gen2:latest`.
- **VM size**: `Standard_D2s_v5` (2 vCPU / 8 GB RAM) is the minimum. DuckDB
  and pyarrow load the active session's parquet shards into memory, and the
  OS plus the Next.js frontend eat ~1 GB before the backend starts.
  `Standard_D4s_v5` (16 GB) is the comfortable size for a busy single-tenant
  deploy.
- **OS disk**: 64 GB Premium SSD (P10) is sufficient for the OS plus
  container images.
- **Data disk**: attach a separate managed disk (128-512 GB Premium SSD)
  and mount at `/mnt/app-data`. Azure's ephemeral OS disk option is not
  appropriate for the data directory — use a managed disk so the data
  survives VM stop/dealloc.
- **Metadata service (IMDS)**: lives at `169.254.169.254` (same link-local
  IP as AWS and GCE). The backend's SSRF gates in `backend/models/lake.py`
  and `backend/utils/remote_access.py` block outbound requests to this
  address from any code path. **Do not** disable the SSRF gates.
- **NSG rules** (inbound, attached to the NIC or subnet):
  - `tcp/443` from Fastly's published v4 CIDR ranges (see `Caddyfile`)
  - `tcp/80` from Fastly's published v4 CIDR ranges (origin pulls)
  - `tcp/22` from your bastion or admin IP only — operators reach `/admin`
    via SSH tunnel (the frontend middleware blocks `/admin` when the
    Caddy proxy marker header is present)
  - outbound: allow `tcp/443` to the internet (backend pulls from Fastly
    Object Storage). Azure's default outbound rule already permits this,
    but check if your subscription has a custom NSG that locks egress.

## 2. Docker install

```sh
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg git
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
  sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo $VERSION_CODENAME) stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
sudo usermod -aG docker $USER
# Log out and back in so the group membership applies.
docker compose version  # confirm v2.x
```

## 3. Volume mount

```sh
# After attaching the data disk in the portal (typically shows as /dev/sdc):
lsblk  # confirm the device
sudo mkfs.ext4 /dev/sdc
sudo mkdir -p /mnt/app-data
sudo mount /dev/sdc /mnt/app-data

# Persist across reboots:
echo "UUID=$(sudo blkid -s UUID -o value /dev/sdc) /mnt/app-data ext4 defaults,nofail 0 2" \
  | sudo tee -a /etc/fstab

sudo chown -R $USER:$USER /mnt/app-data
mkdir -p /mnt/app-data/{data,cache,configs}
```

Either edit `docker-compose.yml` to point its volumes at `/mnt/app-data`, or
keep the repo at `/mnt/app-data/fastly-log-analytics` so the relative `./data`
paths already resolve to the managed disk.

## 4. Caddy / SSL

Fastly terminates TLS at the edge and reverse-proxies to the origin on `:80`,
so Caddy on the VM speaks plain HTTP (see `Caddyfile`'s `auto_https off`).

If you also want a direct LE certificate (for a staging host that bypasses
Fastly), drop `auto_https off` and replace `:80 {` with `your.host {`.
LE's HTTP-01 challenge needs port 80 reachable from the public internet —
open the NSG to `Internet` (Azure service tag) for `tcp/80` during the
cert handshake. For DNS-01 with Cloudflare, add the Caddy `cloudflare`
DNS module to the custom Caddy image and set `CLOUDFLARE_API_TOKEN` in
the env file.

Azure also offers Azure Front Door + App Service as a managed TLS terminator
if you want to skip Fastly. The stack does not care — it just sees plain
HTTP on `:80` either way.

## 5. First deploy + restart flow

```sh
cd /mnt/app-data
git clone https://github.com/fastly/fastly-log-analytics.git
cd fastly-log-analytics
# Copy configs from your local dev box or restore from blob storage backup.
docker compose up -d --build
```

The repeat-deploy flow is the platform-agnostic `restart.sh` pattern:

```sh
#!/usr/bin/env bash
# ~/restart.sh on the VM
set -euo pipefail
cd /mnt/app-data/fastly-log-analytics
git pull
docker compose up -d --build
sleep 10
curl -fsS http://localhost:8000/api/health
```

After a force-push to the deploy branch, pre-flight with
`git fetch && git reset --hard origin/<branch>` before running `restart.sh`.

### Optional systemd unit

```ini
# /etc/systemd/system/fastly-log-analytics.service
[Unit]
Description=Fastly Log Analytics docker compose stack
Requires=docker.service
After=docker.service network-online.target
Wants=network-online.target

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=/mnt/app-data/fastly-log-analytics
ExecStart=/usr/bin/docker compose up -d
ExecStop=/usr/bin/docker compose down

[Install]
WantedBy=multi-user.target
```

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now fastly-log-analytics
```

## 6. Secrets management

The backend reads Fastly Object Storage credentials from environment variables.
Three options, in order of preference:

1. **Azure Key Vault + system-assigned managed identity**. Enable a managed
   identity on the VM, grant it `get` on the specific Key Vault secret
   (`get` only — not `list`), and use a wrapper script that calls
   `az keyvault secret show` (or `curl` against IMDS for the access token,
   then the Key Vault REST API) to export the secret before
   `docker compose up`. **This is the preferred option on Azure** — no
   long-lived credentials touch the VM disk, and rotating the secret in
   Key Vault means the next `restart.sh` picks up the new value with no
   redeploy.
2. **Service principal client secret in a `.env` file**. Less preferred —
   you now have a long-lived credential on disk that itself unlocks the
   Key Vault. Use this only if the VM cannot use managed identity (some
   subscription policies forbid it).
3. **`.env` file with the Fastly credentials directly**,
   `chmod 600`. Simplest; acceptable for solo-dev deploys. Not acceptable
   if multiple operators share the VM.

**When to prefer managed identity over baked-in service account creds**:
always, unless something blocks it. Managed identity removes the
credential-rotation problem entirely — the IMDS-provided token is short-lived
(rotated every hour) and never lands on disk. Baked-in creds (option 3) only
make sense for a solo-dev environment where the operational simplicity wins
over the marginal security delta.

Do **not** bake credentials into the docker image — the image is built from a
public repo.

## 7. Post-deploy verification

```sh
# Backend up?
curl -fsS http://localhost:8000/api/health

# Frontend up?
curl -fsSI http://localhost:3000 | head -1

# Caddy fronting both?
curl -fsS http://localhost/api/health

# End-to-end through Fastly:
curl -fsS https://your.fastly.host/api/health

# Logs:
docker compose logs --tail 100 backend
docker compose logs --tail 100 frontend
docker compose logs --tail 100 caddy | jq 'select(.status >= 400)'
```

To reach `/admin` securely with full HTTP/2 (multiplexing) support to prevent browser connection starvation when opening multiple tabs, forward the secure Caddy port `8443` from your laptop:

```sh
ssh -L 8443:127.0.0.1:8443 <user>@<vm-public-ip>
# then browse to https://localhost:8443/admin
```

> [!NOTE]
> Since this uses Caddy's internal self-signed TLS certificates for local loopback verification, your browser will display a certificate warning when you first visit `https://localhost:8443`. It is completely safe to bypass this warning (click "Advanced" -> "Proceed to localhost"), as the connection is running entirely inside your encrypted SSH tunnel.

If you use Azure Bastion instead of a public IP, the tunnel goes through the
Bastion service — see Azure Bastion's native client documentation for the
exact `az network bastion tunnel` invocation.
