# Deploy to Google Compute Engine

This runbook formalizes the current production deploy flow on GCE. The same
docker compose stack runs unchanged on AWS / Azure / bare metal; only the
host-provisioning steps differ.

## 1. Host provisioning

- **Image**: `debian-12-bookworm-v*` or `ubuntu-2204-jammy-v*`. Both ship a
  recent enough kernel for the Docker overlay2 driver.
- **Machine type**: `e2-standard-2` (2 vCPU / 8 GB) is the minimum. DuckDB
  and pyarrow load the active session's parquet shards into memory, and the
  OS plus the Next.js frontend eat ~1 GB before the backend starts.
  `e2-standard-4` (16 GB) is the comfortable size for a busy single-tenant
  deploy.
- **Boot disk**: 50 GB pd-balanced is fine for the OS plus container images.
- **Data disk**: attach a separate persistent disk (100-500 GB pd-balanced
  depending on cache retention) and mount at `/mnt/app-data`. The data
  directory must live on a persistent disk — the boot disk is fine for
  software but a separate disk lets you snapshot data without snapshotting
  the OS.
- **Metadata service**: GCE's metadata service lives at `169.254.169.254`
  (the same link-local IP as AWS and Azure). The backend's SSRF probe in
  `backend/models/lake.py` and `backend/utils/remote_access.py` blocks
  outbound requests to this address from any code path. **Do not** disable
  the SSRF gates.
- **Firewall rules** (VPC firewall, applied via tags):
  - `tcp/443` from Fastly's published v4 CIDR ranges (see `Caddyfile`)
  - `tcp/80` from Fastly's published v4 CIDR ranges (origin pulls)
  - `tcp/22` from your bastion or admin IP only — operators reach `/admin`
    via SSH tunnel (the frontend middleware blocks `/admin` when the Caddy
    proxy marker header is present)
  - egress: all (backend pulls from Fastly Object Storage over HTTPS)

## 2. Docker install

```sh
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg git
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/debian/gpg | \
  sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/debian $(. /etc/os-release && echo $VERSION_CODENAME) stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
sudo usermod -aG docker $USER
# Log out and back in so the group membership applies.
docker compose version  # confirm v2.x
```

## 3. Volume mount

```sh
# After attaching the data disk in the console (it shows up as /dev/sdb):
sudo mkfs.ext4 /dev/sdb
sudo mkdir -p /mnt/app-data
sudo mount /dev/sdb /mnt/app-data

# Persist across reboots:
echo "UUID=$(sudo blkid -s UUID -o value /dev/sdb) /mnt/app-data ext4 defaults,nofail 0 2" \
  | sudo tee -a /etc/fstab

sudo chown -R $USER:$USER /mnt/app-data
mkdir -p /mnt/app-data/{data,cache,configs}
```

Either edit `docker-compose.yml` to point its volumes at `/mnt/app-data`, or
keep the repo at `/mnt/app-data/fastly-log-analytics` so the relative `./data`
paths already resolve to the persistent disk.

## 4. Caddy / SSL

Fastly terminates TLS at the edge and reverse-proxies to the origin on `:80`,
so Caddy on the VM speaks plain HTTP (see `Caddyfile`'s `auto_https off`).

If you also want a direct LE certificate (for a staging host that bypasses
Fastly), drop `auto_https off` and replace `:80 {` with `your.host {`.
LE's HTTP-01 challenge needs port 80 reachable from the public internet —
open the firewall to `0.0.0.0/0` for `tcp/80` during the cert handshake.
For DNS-01 with Cloudflare, add the Caddy `cloudflare` DNS module to the
custom Caddy image and set `CLOUDFLARE_API_TOKEN` in the env file.

## 5. First deploy + restart flow

```sh
cd /mnt/app-data
git clone https://github.com/fastly/fastly-log-analytics.git
cd fastly-log-analytics
# Copy configs from your local dev box or restore from a GCS backup.
docker compose up -d --build
```

The repeat-deploy flow is the existing `~/restart.sh` pattern (canonicalized
here so it works on every platform).

**Do not use a bare `docker compose up -d --build` for a redeploy.** It takes
the whole stack down, including Caddy — and Caddy is both the sole ingress and
the thing that serves the styled auto-refreshing "updating" page (see
`handle_errors` in the `Caddyfile`). With Caddy down, visitors get the
browser's connection-refused error for the length of the deploy instead of
that page. Three specifics make the difference between ~45 s of downtime and
~1 s:

- **Build before stopping anything.** The build is the slow part; the old
  containers serve throughout it.
- **`--no-deps`, and never rebuild Caddy.** Caddy carries
  `depends_on: frontend`, so a plain `up -d` recreates it every deploy even
  though its image is unchanged. Worse, `compose build` with no service list
  rebuilds it too, and `caddy/Dockerfile` runs `xcaddy build`, which
  recompiles and yields a **new image id every time** — so compose then
  recreates it on principle. Build only `backend frontend`. A `Caddyfile`
  edit only needs `caddy reload` (zero downtime); a `caddy/` edit is the one
  case that justifies a rebuild.
- **Recreate backend and frontend as two separate commands.** The base
  compose has `frontend depends_on backend: condition: service_healthy`, so
  naming both in one `up` makes compose hold the frontend until the backend
  healthcheck passes — roughly 40 s of extra downtime on its own.

```sh
#!/usr/bin/env bash
# ~/restart.sh on the VM
set -uo pipefail
cd /mnt/app-data/fastly-log-analytics
COMPOSE=(docker compose -f docker-compose.yml -f docker-compose.prod.yml)

BEFORE="$(git rev-parse HEAD)"
# Attempt pull, but if it fails (e.g. after a history squash/force-push),
# fall back to git fetch + git reset --hard to match origin exactly.
if ! git pull; then
  echo "git pull failed (likely due to force-push/diverged history); resetting to origin..."
  git fetch origin && git reset --hard "origin/$(git rev-parse --abbrev-ref HEAD)" || exit 1
fi
CHANGED="$(git diff --name-only "$BEFORE" HEAD)"


# 1. Build the app images while the current stack keeps serving.
"${COMPOSE[@]}" build backend frontend || {
  echo "build failed — nothing stopped, old stack still serving"; exit 1; }

# 2. Swap the app containers. Separate commands so compose can't serialize
#    the frontend behind the backend healthcheck.
"${COMPOSE[@]}" up -d --no-deps backend
"${COMPOSE[@]}" up -d --no-deps frontend

# 3. Touch Caddy only if it actually changed.
if echo "$CHANGED" | grep -q '^caddy/'; then
  "${COMPOSE[@]}" build caddy && "${COMPOSE[@]}" up -d --no-deps caddy
elif echo "$CHANGED" | grep -q '^Caddyfile$'; then
  docker exec app-caddy-1 caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile \
    || "${COMPOSE[@]}" up -d --no-deps caddy
fi

# 4. Poll the SAME readiness signal as the Docker healthcheck in
# docker-compose.prod.yml — NOT `curl .../api/health`, which returns 200 the
# moment the HTTP server binds and so reports "ready" within seconds while the
# per-service warm-up loop (backend/main.py:_background_startup) still has
# 15-20 min to run. Check the response BODY for the top-level "status":"ok".
echo "site is serving; waiting for warm-up (can take 15-20 min, safe to Ctrl-C)..."
for i in $(seq 1 100); do  # 100 * 15s = 25 min budget, matches compose start_period
  if curl -fsS 'http://localhost:8000/api/health?deep=1' 2>/dev/null | grep -q '^{"status":"ok"'; then
    echo "backend warm after $((i * 15))s"
    exit 0
  fi
  sleep 15
done
echo "backend did not report ready within 25 min — check: curl -s 'http://localhost:8000/api/health?deep=1'" >&2
exit 1
```

An already-loaded browser tab doesn't do a full navigation, so it never sees
Caddy's page — its in-flight XHR just fails. `useBootstrap` therefore polls
while (and only while) the bootstrap query is errored, so an open tab
reconnects on its own once the backend is back.

**After a force-push** to the deploy branch:

```sh
git fetch && git reset --hard origin/<branch>
~/restart.sh
```

The browser needs a hard-refresh after a frontend rebuild — Caddy and the
Next.js static asset hashes are cached aggressively.

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

1. **Google Secret Manager** + a wrapper script that exports the values before
   `docker compose up`. The instance's default service account needs
   `roles/secretmanager.secretAccessor` on the specific secret only.
2. **Instance service account + service account key file** mounted into the
   container. Less preferred — long-lived JSON keys are a known compromise
   vector. If you must, restrict the SA's IAM role to the minimum (read one
   secret).
3. **`.env` file at `/mnt/app-data/fastly-log-analytics/.env`** with
   `chmod 600`. Simplest; acceptable for solo-dev deploys. Not acceptable
   if multiple operators share the VM.

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
ssh -L 8443:127.0.0.1:8443 <user>@<instance-external-ip>
# then browse to https://localhost:8443/admin
```

> [!NOTE]
> Since this uses Caddy's internal self-signed TLS certificates for local loopback verification, your browser will display a certificate warning when you first visit `https://localhost:8443`. It is completely safe to bypass this warning (click "Advanced" -> "Proceed to localhost"), as the connection is running entirely inside your encrypted SSH tunnel.

The frontend middleware sees no `X-Proxied-By-Caddy` header on the tunneled
connection (which presents a loopback IP) and serves `/admin`.

## 8. RUM: pin the self-hosted Faro Web SDK version

Before self-hosting, the RUM tracker loaded the Faro Web SDK from jsDelivr
with a floating `@^1` range — which resolved to `1.19.0` as of that writing.
That CDN load no longer exists in any form: the generated tracker JS
(`backend/provision/rum_assets.py::generate_rum_tracker_js`) unconditionally
loads the Faro SDK from `/js/faro-sdk.js` — a relative, first-party path on
the service's own domain, served from FOS via the RUM asset-fetch VCL. There
is no CDN fallback and no code path that can produce one.

Because of that, RUM can no longer be enabled without a self-hosted bundle
behind `/js/faro-sdk.js`. `enable_rum()` pins an explicit version
(`cfg["rum"]["faro_version"]`) when given one, and otherwise defaults to
`backend.core.faro_versions.DEFAULT_FARO_VERSION` (currently `2.10.0`,
npm's `dist-tags.latest` as of this task) — enabling RUM always downloads,
integrity-verifies, and uploads a bundle to the service's FOS bucket.
A service that was enabled *before* this default existed and still has no
`faro_version` self-heals on its next RUM sync cron tick: the cron adopts
`DEFAULT_FARO_VERSION`, uploads the bundle, persists the pin, AND
reconciles the service's deployed VCL so the `/js/faro-sdk.js` route
(previously absent — that service's VCL was generated with
`faro_version=None`) actually goes live. All of that happens automatically;
it never depends on this script being run manually.

**What this script is for**: pin a service to a *specific* version instead
of the default — a known-good version that won't move when the default
changes, or rolling back to an older one.

**When to run it**: any time you want to move a service to a specific
version outside of the admin UI's upgrade flow. You do not need to run it
just to "turn on" self-hosting — enabling RUM already does that
automatically.

```sh
# Pins to 2.10.0 (current npm dist-tags.latest) by default:
scripts/pin-rum-faro-version.sh <service_id>

# Pin to an explicit version instead:
scripts/pin-rum-faro-version.sh <service_id> 2.10.0
scripts/pin-rum-faro-version.sh <service_id> 1.19.0
```

Replace `<service_id>` with the real Fastly logging service ID — never
commit that ID into this repo (it's public). The script is idempotent (a
repeat run against an already-correct pin is a no-op), writes atomically,
refuses to run if the config is missing or not valid JSON, and never
touches sibling keys under `cfg["rum"]` — `faro_content_hash` and
`faro_fos_etag_md5` are owned by the reconcile cron, and clobbering them
would make the cron think the bundle it already uploaded is stale and
re-upload it on every tick.

Pinning only updates the config value. To actually fetch and upload the
bundle for the new version, either:

- wait for the RUM reconcile cron's next tick, which detects the version
  mismatch and uploads the pinned version automatically, or
- use the admin RUM page's upgrade flow (`/admin/rum`) to trigger it
  immediately and watch progress live via its SSE log stream.

**Moving between versions later**: the admin RUM page is the normal path —
it shows the currently deployed version, lets you pick a target from the
available versions list, and streams upload/reconcile progress. Use this
script only when you need to set the pin from outside that UI (e.g.
scripting a fresh-VM bring-up, or recovering a config by hand).
