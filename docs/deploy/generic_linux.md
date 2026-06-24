# Deploy to a generic Linux VM (bare metal, Linode, DigitalOcean, Hetzner, etc.)

This runbook covers running the stack on any Linux host that does not provide
a cloud metadata service or a vendor secrets manager. The backend image is
the same as every other platform; only the host-provisioning steps differ.

## 1. Host provisioning

- **Distro**: Debian 12, Ubuntu 22.04, or any recent systemd-based distro
  that has a current `docker-ce` package. Alpine works but requires the
  community Docker package and a couple of glibc workarounds — stick with
  glibc distros for the production VM.
- **Sizing**: 2 vCPU / 8 GB RAM is the minimum. DuckDB and pyarrow load the
  active session's parquet shards into memory, and the OS plus the Next.js
  frontend eat ~1 GB before the backend starts. 4 vCPU / 16 GB RAM is the
  comfortable size for a busy single-tenant deploy. Provider-specific
  starting points:
  - Linode: `g6-standard-2` (4 GB) is too small; use `g6-standard-4` (8 GB)
    or `g6-standard-6` (16 GB).
  - DigitalOcean: `s-2vcpu-8gb` (Basic) or `g-2vcpu-8gb` (General Purpose).
  - Hetzner: `CX32` (4 GB) is too small; use `CX42` (8 GB) or `CCX13`
    (Dedicated 16 GB) for headroom.
  - Bare metal: any box with 8 GB+ RAM, an SSD, and a 1 Gbps NIC.
- **Storage**: a 100 GB+ SSD for the OS and container images, mounted as
  `/`. Mount the data directory at `/mnt/app-data` — either as a separate
  block-storage volume (Linode Block Storage, DO Volumes, Hetzner Volumes)
  or as a directory on the root disk if the provider doesn't offer
  attachable volumes. **A separate volume is strongly preferred** — it lets
  you snapshot, resize, and migrate the data independently of the OS.
- **No metadata service guarantees**: bare-metal and most VPS providers do
  not expose a `169.254.169.254` metadata endpoint. Even when they do
  (DigitalOcean droplets have one), it typically returns only network
  configuration, never credentials. The backend's SSRF gates in
  `backend/models/lake.py` and `backend/utils/remote_access.py` still
  block outbound requests to the link-local range as defense in depth.
- **Firewall**: use `ufw` (Debian/Ubuntu) or the provider's network
  firewall. Required rules:
  - `tcp/443` from Fastly's published v4 CIDR ranges (see `Caddyfile`)
  - `tcp/80` from Fastly's published v4 CIDR ranges (origin pulls)
  - `tcp/22` from your home or bastion IP only — operators reach `/admin`
    via SSH tunnel
  - egress: all (backend pulls from Fastly Object Storage over HTTPS)

  Example `ufw` setup:

  ```sh
  sudo ufw default deny incoming
  sudo ufw default allow outgoing
  sudo ufw allow from <your-admin-ip> to any port 22
  # Repeat per Fastly CIDR; or front the box with a cloud LB that does it.
  sudo ufw allow from 199.232.0.0/16 to any port 80
  sudo ufw allow from 199.232.0.0/16 to any port 443
  # ... etc for the rest of the Fastly ranges
  sudo ufw enable
  ```

## 2. Docker install

```sh
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg git
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/$(. /etc/os-release && echo $ID)/gpg | \
  sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/$(. /etc/os-release && echo $ID) \
  $(. /etc/os-release && echo $VERSION_CODENAME) stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
sudo usermod -aG docker $USER
# Log out and back in so the group membership applies.
docker compose version  # confirm v2.x
```

## 3. Volume mount

If your provider offers attachable block storage:

```sh
# Confirm the device with `lsblk` (Linode: /dev/sdc, DO: /dev/disk/by-id/...,
# Hetzner: /dev/disk/by-id/scsi-...)
sudo mkfs.ext4 /dev/sdX
sudo mkdir -p /mnt/app-data
sudo mount /dev/sdX /mnt/app-data
echo "UUID=$(sudo blkid -s UUID -o value /dev/sdX) /mnt/app-data ext4 defaults,nofail 0 2" \
  | sudo tee -a /etc/fstab
```

If the host has only a root disk:

```sh
sudo mkdir -p /mnt/app-data
# Same path; just no separate device. Snapshots and resize now require
# resizing the root volume.
```

Either way:

```sh
sudo chown -R $USER:$USER /mnt/app-data
mkdir -p /mnt/app-data/{data,cache,configs}
```

## 4. Caddy / SSL

Fastly terminates TLS at the edge and reverse-proxies to the origin on `:80`,
so Caddy on the VM speaks plain HTTP (see `Caddyfile`'s `auto_https off`).

If you also want a direct LE certificate (for a staging host that bypasses
Fastly), drop `auto_https off` and replace `:80 {` with `your.host {`.
LE's HTTP-01 challenge needs port 80 reachable from the public internet —
temporarily open the firewall to `0.0.0.0/0` for `tcp/80` during the cert
handshake. For DNS-01 with Cloudflare, add the Caddy `cloudflare` DNS module
to the custom Caddy image and set `CLOUDFLARE_API_TOKEN` in the env file.

DNS-01 is the recommended path on bare metal — you do not have to open port
80 to the public internet, and the cert renews unattended every 60 days.

## 5. First deploy + restart flow

```sh
cd /mnt/app-data
git clone https://github.com/fastly/fastly-log-analytics.git
cd fastly-log-analytics
# Copy configs from your local dev box or restore from a backup bucket.
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

No vendor secrets manager exists on bare metal / generic VPS. Credentials
come from an env file. Two patterns, in order of preference:

1. **`.env` file at `/mnt/app-data/fastly-log-analytics/.env`** with
   `chmod 600` and owned by the deploy user. `docker compose` reads this
   file automatically when starting the stack. The file lives on the data
   disk so it survives an OS reinstall but not a disk loss — back it up
   alongside any other secrets you care about (1Password, Bitwarden,
   `pass`, an encrypted git repo).
2. **External secrets fetcher**: a wrapper script that pulls from a
   self-hosted Vault / Doppler / Infisical / Bitwarden CLI before invoking
   `docker compose up`. Use this if you have more than one operator
   sharing the VM — it keeps the long-lived secret off the box entirely.

Whichever you pick, **do not** bake credentials into the docker image — the
image is built from a public repo.

### Backing up `.env`

A common mistake: deploy works, env file is on the VM, then six months later
the disk dies and the only copy of the Fastly access key is gone. Mitigation:

```sh
# On your laptop, immediately after first deploy:
scp deploy@your-vm:/mnt/app-data/fastly-log-analytics/.env ~/secrets/vm-env-backup
# Then store ~/secrets/vm-env-backup in your password manager.
```

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

To reach `/admin`, run on your laptop:

```sh
ssh -L 8080:127.0.0.1:3000 <user>@<vm-ip>
# then browse to http://localhost:8080/admin
```

The frontend middleware sees no `X-Proxied-By-Caddy` header on the tunneled
connection and serves `/admin`.

### Provider-specific gotchas

- **DigitalOcean**: the default Cloud Firewall opens nothing — you must add
  rules explicitly. The default droplet image has `ufw` installed but
  disabled.
- **Linode**: Linode's Cloud Firewall is separate from `ufw`. Pick one
  layer to own the rules — running both means every change happens twice.
- **Hetzner**: Hetzner's Cloud Firewall is at the network layer; `ufw` is
  on the host. Same advice as Linode.
- **Bare metal**: no provider firewall at all — `ufw` (or `nftables`) is
  the only line of defense. Confirm rules with `sudo ufw status numbered`
  before exposing the box.
