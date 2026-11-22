# Disaster Recovery — Fastly Log Analytics (Cloud & High-Scale)

This runbook covers accidental service deletion and cold-start recovery from Fastly Object Storage (FOS) for **Fastly Log Analytics Cloud (Compute)**. For **High-Scale** Kubernetes capacity and cold-start recovery, see [docs/runbooks/high-scale-capacity-and-recovery.md](runbooks/high-scale-capacity-and-recovery.md).

## Failure Scenarios & Recovery Modes (FLP Cloud)

| Scenario | What Survived | Recovery Mode | Outcome |
|---|---|---|---|
| **Mode A — Service Deletion** | KV Store (`flp_state`), Secret Store (`flp_secrets`), Config Store (`flp_config`), and FOS bucket survive; one or more Compute/Worker/Front services were accidentally deleted. | `provision.py recover --mode relink` followed by `make cloud-deploy` | Recreates only the deleted service(s), links them to the surviving stores, updates `flp_config`, and preserves all users, passkeys, saved views, watermarks, and historical data. Zero data loss. |
| **Mode B — Full Compute + KV Store Loss** | Only the **FOS bucket** (`processed/*` and `meta/cloud_dr_state.json`) survives; Compute services and `flp_state` KV Store are gone. | Fresh `bootstrap` + `make cloud-deploy` + Admin UI **Recover from FOS** or `provision.py recover --mode fos` (`POST /api/admin/provision/recover`) | Mints a fresh bucket-scoped FOS key from the Fastly API key, restores `svc:<sid>`, `views:<sid>`, `ingest:watermark` (`catalog::Head` + `floor_key`), `ingest:rum:watermark`, and `admin:install`, and syncs Front VCL dictionaries so historical `processed/*` dashboards work immediately. |

---

## DR State Mirroring (`meta/cloud_dr_state.json`)

Whenever an active service's configuration or saved views change (`PATCH /api/admin/services/:sid`, job finalization, `POST /api/views`, `DELETE /api/views/:id`), FLP Cloud mirrors a sanitized JSON snapshot to FOS at:

```text
{scope}meta/cloud_dr_state.json
```

where `{scope}` is `""` (empty prefix) or `"{fos_prefix}/"`.

### Telemetry Attribution

All `meta/` object operations are classified under the `pq_ops` dataset (caller `admin:dr_state`) in Usage telemetry rather than `raw_request`.

### Schema (`schema_version: 1`)

```json
{
  "schema_version": 1,
  "updated_at": 1732144800,
  "service_id": "<monitored-vcl-service-id>",
  "service": { "...sanitized non-secret Service config..." },
  "views": [ "...SavedView objects..." ],
  "catalog_head": { "...catalog::Head or null..." },
  "floor_key": "<optional raw floor key>",
  "rum_stats": { "...optional RUM watermark stats..." }
}
```

### Hard Security Guarantees

- **No secrets ever enter FOS.** Before serialization, `compute/cloud/src/admin/dr.rs` explicitly strips `fastly_token_ref`, `fos_access_key_ref`, `fos_secret_key_ref`, `pending_job_id`, `data_reset`, `generation`, `_audit`, `_audit_outbox`, `_audit_outbox_base`, `_job_checkpoint`, and any `_`-prefixed top-level KV metadata fields.
- **Recursive key scrubbing** removes any nested key whose normalized name contains `token`, `secret`, `password`, `passwd`, `passcode`, `api_key`, `access_key`, `secret_key`, `private_key`, `credential`, `fos_access_key`, `fos_secret_key`, `fastly_api_key`, `cdn_key`, `front_key`, or `session_key`.
- **Sentinel-secret unit test** (`admin::dr::tests::dr_state_strips_secret_refs_transient_fields_and_sentinel_secrets`) proves that no sentinel secret can serialize into `meta/cloud_dr_state.json` while legitimate non-secret fields (`cookie_session`, `scoring_keys_store_id`, `floor_key`, `catalog_head`, `views`, `rum_stats`) are preserved.
- **Catalog fallback**: If `meta/cloud_dr_state.json` is missing, corrupt, or has no `catalog_head`, `POST /api/admin/provision/recover` performs a bounded scan of `{scope}processed/_catalog/` (newest objects first, respecting Fastly's backend call budget) to reconstruct a valid `catalog::Head`.

---

## Mode A Runbook — Re-Link Surviving Stores (`--mode relink`)

Use Mode A when one or more Fastly services (`<name>`, `<name>-worker`, `<name>-front`) were accidentally deleted, but the three stores (`<name>_state`, `<name>_secrets`, `<name>_config`) still exist.

### Prerequisites

- Provide `FASTLY_API_TOKEN` (or `FASTLY_API_KEY`) via environment variable or interactive `getpass` prompt. **Never pass tokens as CLI flags.**
- If the Front VCL service was also deleted, decide whether ERL is enabled (`--erl-service-id <vcl-sid>` or `--no-erl`).

### 1. Plan (Dry Run — Default)

```sh
uv run compute/cloud/infra/provision.py recover \
  --mode relink \
  --name flp
```

Dry run makes no network calls, requires no credentials, and outputs `"writes": false`.

### 2. Apply Recovery

```sh
uv run compute/cloud/infra/provision.py recover \
  --mode relink \
  --name flp \
  --inventory compute/cloud/pkg/flp-resources.local.json \
  --no-erl \
  --apply
```

Optional store ID overrides if store names differ from `<name>_state` / `<name>_secrets` / `<name>_config`:

```sh
  --kv-store-id <kv-id> \
  --secret-store-id <secret-id> \
  --config-store-id <config-id>
```

### 3. Deploy & Verify

```sh
make cloud-deploy NAME=flp
```

- `recover --mode relink` sets `admin_code_issued: true` in the recovered inventory so `make cloud-deploy` does **not** rotate `admin_setup_hash` on your surviving Secret Store.
- **If the Front VCL service was recreated**: Fastly Secret Stores are write-only, so `recover --mode relink` automatically rotates only `front_key` and `cdn_key` in both `flp_secrets` and the new `flp_front_auth` dictionary (leaving `session_key`, `worker_secret`, `admin_setup_hash`, and per-service FOS keys untouched). After `make cloud-deploy`, open `/admin` and run **Reconcile** to repopulate per-service `flp_fos_buckets`, `flp_fos_credentials`, and any regional FOS backend on the new Front VCL service.

---

## Mode B Runbook — FOS-Only Cold-Start Recovery (`--mode fos`)

Use Mode B when both the Compute services **and** the `flp_state` KV Store are gone, leaving only the FOS bucket containing historical `processed/*` data and `meta/cloud_dr_state.json`.

### 1. Bootstrap & Deploy a Fresh Cloud Install

1. Create fresh services and stores:
   ```sh
   uv run compute/cloud/infra/provision.py bootstrap \
     --name flp \
     --erl-service-id <existing-active-vcl-service-id> \
     --inventory compute/cloud/pkg/flp-resources.local.json \
     --apply
   ```
2. Build and deploy:
   ```sh
   make cloud-build
   make cloud-deploy NAME=flp
   ```
3. Open `https://<install>.global.ssl.fastly.net/setup`, enter the one-time setup code printed by `make cloud-deploy`, and register your admin passkey.

### 2. Trigger Cold-Start Recovery

#### Option 1 — Admin UI

1. Sign in and open `/admin`.
2. When 0 services are provisioned, click **Recover from FOS**.
3. Enter your **Fastly API key**, monitored **Service ID**, **FOS Bucket**, **FOS Region**, and optional **FOS Prefix** (or expand the fallback section to supply explicit FOS access/secret keys).

#### Option 2 — CLI (`provision.py recover --mode fos`)

1. Dry run first (no credentials, no network calls):
   ```sh
   uv run compute/cloud/infra/provision.py recover \
     --mode fos \
     --base-url https://<install>.global.ssl.fastly.net \
     --service-id <monitored-vcl-service-id> \
     --bucket <fos-bucket> \
     --region <fos-region>
   ```
   (Pass `--inventory compute/cloud/pkg/<install>-resources.local.json` instead of `--base-url` to derive the origin automatically.)

2. Export credentials (never pass secrets as CLI flags) and apply:
   ```sh
   export FASTLY_API_TOKEN="<fastly-api-token>"
   export FLP_ADMIN_COOKIE="<flp_session-cookie-value-from-admin-browser>"
   uv run compute/cloud/infra/provision.py recover \
     --mode fos \
     --base-url https://<install>.global.ssl.fastly.net \
     --service-id <monitored-vcl-service-id> \
     --bucket <fos-bucket> \
     --region <fos-region> \
     --apply
   ```
   To supply explicit FOS credentials instead of minting a fresh bucket-scoped key from the Fastly API token, add `--use-fos-keys` and provide `FOS_ACCESS_KEY` and `FOS_SECRET_KEY` via environment or prompt.

#### Direct API Contract (`POST /api/admin/provision/recover`)

Alias: `POST /api/provision/recover`. Requires Admin `Cookie: flp_session=<token>`, `Origin: <public_base_url>`, and `Content-Type: application/json`.

Request:
```json
{
  "token": "<fastly-api-token>",
  "service_id": "<monitored-vcl-service-id>",
  "fos_bucket": "<fos-bucket>",
  "fos_region": "us-east-1",
  "fos_prefix": "",
  "fos_access_key": "<optional>",
  "fos_secret_key": "<optional>"
}
```

Response (`200 OK`):
```json
{
  "ok": true,
  "service_id": "<monitored-vcl-service-id>",
  "source": "dr_state",
  "views_restored": 2,
  "catalog_generation": 42,
  "front_synced": true
}
```

`source` is `"dr_state"` when restored from `meta/cloud_dr_state.json`, `"catalog_scan"` when reconstructed from `processed/_catalog/`, or `"minimal"` when the bucket had no catalog metadata yet.

### 3. Post-Recovery Verification

1. Open `/admin` and verify the recovered service shows `active` with its restored settings.
2. Open the main dashboard and verify historical `processed/*` data and saved views load immediately.
3. Run **Reconcile** in `/admin` if the monitored VCL logging endpoint needs verification or if the recovered bucket uses a non-default FOS region whose `fos_<region>` backend has not yet been added to the new Front VCL service.
