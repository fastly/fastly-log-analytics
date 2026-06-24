# Deploy to AWS EC2

This runbook covers running the stack on an Amazon Linux 2023 EC2 instance with
Docker + docker compose. The backend image is the same as every other platform;
only the host-provisioning steps differ.

## 1. Host provisioning

- **AMI**: Amazon Linux 2023 (al2023-ami-*). Ubuntu 22.04 also works — switch
  `dnf` for `apt` in the install steps if you prefer it.
- **Instance type**: `t3.large` (2 vCPU / 8 GB RAM) is the minimum. DuckDB and
  pyarrow load the active session's parquet shards into memory, and the OS plus
  the Next.js frontend eat ~1 GB before the backend starts. `t3.xlarge`
  (16 GB) is the comfortable size for a busy single-tenant deploy.
- **EBS volume**: a single 100 GB gp3 root volume is sufficient for the OS plus
  the container images. Attach a second gp3 volume (100-500 GB depending on
  cache retention) and mount it at `/mnt/app-data`. The durable data directory
  must be on EBS — the instance store on `t3` types is ephemeral and will
  vanish on stop/start.
- **IMDSv2**: Amazon Linux 2023 defaults to IMDSv2 (session-token required).
  The backend's SSRF probe in `backend/models/lake.py` already handles this —
  it does not call the metadata service in production paths, only the SSRF
  test does, and that test treats both IMDSv1 (`GET`) and IMDSv2
  (`PUT /latest/api/token` first) as equivalent untrusted endpoints. **Do not
  re-enable IMDSv1** on the instance; if you ever need to read instance
  metadata for debugging, use:

  ```sh
  TOKEN=$(curl -X PUT "http://169.254.169.254/latest/api/token" \
    -H "X-aws-ec2-metadata-token-ttl-seconds: 60")
  curl -H "X-aws-ec2-metadata-token: $TOKEN" \
    http://169.254.169.254/latest/meta-data/instance-id
  ```

- **Security group rules** (inbound):
  - `tcp/443` from Fastly's published v4 CIDR ranges (see `Caddyfile`)
  - `tcp/80` from Fastly's published v4 CIDR ranges (origin pulls)
  - `tcp/22` from your bastion or admin IP only — the SSH port-forward is
    how operators reach `/admin` (the frontend middleware blocks `/admin`
    when the Caddy proxy marker header is present, so admin traffic must
    bypass Caddy via SSH tunnel)
  - egress: all (the backend pulls from Fastly Object Storage over HTTPS)

## 2. Docker install

```sh
sudo dnf install -y docker git
sudo systemctl enable --now docker
sudo usermod -aG docker ec2-user
# Log out and back in so the group membership applies.

# Compose v2 plugin (Amazon Linux 2023 packages it as docker-compose-plugin):
sudo dnf install -y docker-compose-plugin
docker compose version  # confirm v2.x
```

## 3. Volume mount

```sh
# After attaching a second EBS volume in the console:
sudo mkfs.ext4 /dev/nvme1n1     # confirm device with `lsblk` first
sudo mkdir -p /mnt/app-data
sudo mount /dev/nvme1n1 /mnt/app-data

# Persist across reboots:
echo "UUID=$(sudo blkid -s UUID -o value /dev/nvme1n1) /mnt/app-data ext4 defaults,nofail 0 2" \
  | sudo tee -a /etc/fstab

sudo chown -R ec2-user:ec2-user /mnt/app-data
mkdir -p /mnt/app-data/{data,cache,configs}
```

Update `docker-compose.yml`'s volume mounts to reference `/mnt/app-data` (or
keep the repo at `/mnt/app-data/fastly-log-analytics` so the relative `./data`
paths already resolve to the EBS mount).

## 4. Caddy / SSL

Fastly terminates TLS at the edge and reverse-proxies to the origin on `:80`,
so Caddy on the VM speaks plain HTTP (see `Caddyfile`'s `auto_https off`).

If you also want a direct LE certificate (e.g. for a staging host that bypasses
Fastly), drop the `auto_https off` line and replace `:80 {` with `your.host {`.
LE's HTTP-01 challenge needs port 80 reachable from the public internet — open
the security group to `0.0.0.0/0` for `tcp/80` during the cert handshake. For
DNS-01 (Cloudflare), add the Caddy `cloudflare` DNS module to the custom Caddy
image and set the `CLOUDFLARE_API_TOKEN` env var.

## 5. First deploy + restart flow

```sh
cd /mnt/app-data
git clone https://github.com/fastly/fastly-log-analytics.git
cd fastly-log-analytics
# Copy configs from your local dev box or restore from S3 backup.
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

If you want the stack to come up after a reboot before any user logs in (the
`restart: unless-stopped` policy on the containers will do this once Docker
starts, but a unit gives you `systemctl status` visibility):

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
User=ec2-user

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

1. **AWS Secrets Manager + `aws secretsmanager get-secret-value`** in a wrapper
   script that exports the values before `docker compose up`. Lowest blast
   radius — the secret never lands on disk.
2. **EC2 instance profile** with an IAM role that can read a single secret
   from Secrets Manager. The wrapper script uses the instance's IAM role, so
   the secret never has long-lived AWS keys on the box.
3. **`.env` file at `/mnt/app-data/fastly-log-analytics/.env`** with
   `chmod 600 ec2-user:ec2-user`. Simplest, but the secret sits at rest on
   the EBS volume. Acceptable for solo-dev deploys; not acceptable if you
   have multiple admin operators.

Do **not** bake credentials into the docker image — the image is built from a
public repo and the registry layer is content-addressed, so any baked secret
leaks forever.

## 7. Post-deploy verification

```sh
# Backend up?
curl -fsS http://localhost:8000/api/health

# Frontend up?
curl -fsSI http://localhost:3000 | head -1

# Caddy fronting both?
curl -fsS http://localhost/api/health

# End-to-end through Fastly (replace with your hostname):
curl -fsS https://your.fastly.host/api/health

# Logs:
docker compose logs --tail 100 backend
docker compose logs --tail 100 frontend
docker compose logs --tail 100 caddy | jq 'select(.status >= 400)'
```

If `/api/health` returns 200 but `/admin` returns 403 via the public URL,
that is correct — the admin surface only opens for SSH-port-forwarded
connections (no `X-Proxied-By-Caddy` header). To reach `/admin`, run on your
laptop:

```sh
ssh -L 8080:127.0.0.1:3000 ec2-user@<instance-public-ip>
# then browse to http://localhost:8080/admin
```
