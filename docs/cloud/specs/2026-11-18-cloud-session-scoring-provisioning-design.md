# Cloud Session-Scoring Provisioning: design

Status: draft for review, 2026-11-18.

## Why

Cloud `/admin/session-scoring` is currently read-only (`CloudScoringOverview.tsx`
and `frontend/lib/cloud-engine/scoring-reads.ts` return
`cloud_scoring_management_unsupported` for config/control-plane routes). Operators
can inspect historical `edge_score*` columns from retained request Parquet, but
cannot enable, configure, retrain, rotate keys, or tear down the edge session
scorer in Cloud.

High-Scale (`backend/routers/session_scoring/` +
`backend/provision/session_scoring/`) already implements the full provisioning
and control-plane lifecycle, but relies on assumptions that do not hold on
Fastly Compute (`compute/cloud/`):

1. **Client-supplied or local-file Fastly tokens**: High-Scale reads tokens from
   local `service_config/<sid>.json` or request headers/bodies. Cloud browser
   requests send no Fastly credential for routine operations.
2. **Local state (`svcconfig`, local `matrix.json`, Postgres)**: High-Scale
   stores scoring metadata, matrices, labels, and audit logs on local disk and
   Postgres. Cloud runs on stateless Compute replicas across POPs.
3. **ClickHouse + Python retrain**: High-Scale's `/scoring/retrain` queries
   ClickHouse `request_facts` and runs Python `sessionize` / `build_matrix`.
   Cloud stores processed logs as Parquet in FOS and queries them in the
   browser via DuckDB-Wasm.
4. **In-process synchronous Fastly orchestration**: High-Scale runs
   `enable_scoring` / `disable_scoring` / `teardown_scoring_resources` in a
   single long-lived Python process. Compute requests are bounded by a 45s step
   wall budget (`STEP_WALL_BUDGET`) and 32 backend calls (`BackendBudget`), so
   multi-step Fastly API work must run as resumable `Job`s.

This spec defines how to bring full session-scoring provisioning, management,
retraining, and safe teardown to Cloud while preserving High-Scale parity.

## What it does

- **Server-side Fastly credential brokering**: `compute/cloud` uses its
  Secret Store (`fastly_token.<service_id>`) to authenticate Fastly API calls
  on behalf of authenticated Cloud admin sessions, without sending or exposing
  Fastly tokens in browser requests or responses.
- **Durable multi-POP scoring state**: Stores non-secret scoring metadata in KV
  (`svc:<sid>.scoring` with CAS `generation`), secrets in Secret Store and the
  scorer's `scoring_keys_<sid>` ConfigStore, matrices and labels in FOS under
  `processed/scoring/`, and scoring audit entries in Cloud's operational logs.
- **Resumable `Job`-based provisioning & teardown**: Executes `scoring_enable`,
  `scoring_disable`, and `scoring_teardown` (plus integration into
  `POST /api/provision/teardown`) via `compute/cloud`'s `Job` state machine.
- **Cloud-compatible retrain & evaluation**: Uses browser DuckDB-Wasm to scan
  retained request Parquet and train/evaluate the transition matrix, then
  submits the validated matrix to `compute/cloud` to persist in FOS and push to
  the scorer's `scoring_matrix_<sid>` KV Store.
- **Safe deletion ordering**: Strips edge VCL snippets and the
  `session_scorer` backend from the active logging service version *first*;
  aborts without deleting Compute/store resources if VCL stripping fails.

## Non-goals

- Changing the Rust edge scorer (`compute/scorer/`) scoring algorithm, cookie
  format, or KV binary matrix format (`src/matrix.rs`).
- Running DuckDB or Python inside `compute/cloud` for matrix training.
- Sharing a single Compute scorer service across multiple customer logging
  services (each logging service owns its dedicated `Session Scoring Service for
  <sid>` Compute service and stores, matching High-Scale).
- Deleting historical `edge_score*` columns from already-ingested Parquet logs
  when disabling scoring.

## Anti-goals

- Accepting or persisting Fastly API tokens, `request_secret`, or `aes_key_hex`
  in `svc:<sid>` KV records, `job:<id>` records, FOS objects, audit logs, or API
  responses.
- Deleting the owned Compute scorer service or its stores while the logging
  service's active VCL version still routes sub-fetches to `session_scorer`.
- Exceeding Compute's 45s step wall budget or 32-call `BackendBudget` in a
  single request.
- Breaking existing `CLOUD_SCORING_SCAN_ROUTES` Parquet analytics when scoring
  is disabled or unprovisioned.

---

## Architecture

### 1. Server-side scoped Fastly credential brokering

In Cloud, admin authentication uses passkey sessions (`require_admin` in
`compute/cloud/src/shell.rs`). Browser requests to `/api/services/{id}/scoring/*`
do **not** carry a Fastly API token for routine operations.

1. **Primary credential source**: `compute/cloud` resolves
   `context.secret(&service.fastly_token_ref)` (`fastly_token.<service_id>` in
   the install's `secret_store_id`), which is already written during
   `provision` and `teardown`.
2. **Scoped token upgrade / override**: Provisioning the edge scorer requires
   Fastly account entitlements and token scopes for Compute (`type: wasm`),
   Config Stores, KV Stores, and VCL version activation.
   - `POST /api/services/{id}/scoring/enable` and
     `POST /api/provision/teardown` accept an *optional* `token` in the JSON
     body.
   - If `token` is omitted, `compute/cloud` uses `fastly_token_ref`.
   - If `token` is supplied, `compute/cloud` validates it against the logging
     service's `customer_id` (`validate_token`), checks entitlements, and writes
     it to `fastly_token_ref` via `put_secret` before starting the job.
   - If the brokered token lacks required scopes (401/403 from Fastly) or
     `fastly_token_ref` is missing, `compute/cloud` returns `400` / `403` with
     `{"error": "fastly_token_scope_insufficient", "required_scopes": [...]}` so
     the UI can prompt the admin for a one-time scoped token without storing it
     in the browser.
3. **Secret isolation**:
   - `fastly_token_ref` never leaves `compute/cloud`.
   - `request_secret` (HMAC/shared header secret between logging VCL and the
     Compute scorer) is generated inside `compute/cloud` via
     `runtime.random()`, stored in Cloud's Secret Store as
     `scoring_request_secret.<service_id>`, and written to the scorer's
     `scoring_keys_<scoring_sid>` ConfigStore. It is never returned by
     `/scoring/status`, `/scoring/config`, or `GET /api/services`.
   - `aes_key_hex` (AES-256 cookie key) is generated inside `compute/cloud` and
     written **write-only** to `scoring_keys_<scoring_sid>` (`current_key_hex`,
     and `previous_key_hex` on rotation). Cloud never stores `aes_key_hex` in
     its own Secret Store or KV Store and never reads it back.

### 2. Durable shared scoring state across stateless Cloud replicas

All scoring state lives in Fastly-managed durable stores accessible from any
POP:

| Artifact | Storage location | Consistency / format | Secrets? |
| :--- | :--- | :--- | :--- |
| Scoring config & resource IDs | `svc:<sid>` KV (`Service.scoring: Option<ScoringConfig>`) | Optimistic CAS (`generation`) | No |
| Fastly API token | Secret Store (`fastly_token.<sid>`) | Secret propagation check (`put_secret`) | Yes |
| VCL–scorer shared secret | Secret Store (`scoring_request_secret.<sid>`) | Read-back confirmation on write | Yes |
| Scorer AES-256 keys | Scorer ConfigStore `scoring_keys_<scorer_sid>` | Write-only (`current_key_hex`, `previous_key_hex`) | Yes |
| Live scorer runtime flags | Scorer ConfigStore `scoring_config_<scorer_sid>` | `enforce_threshold`, `l2_enforce_enabled`, `l2_enabled_at`, `scoring_enabled_at`, `debug_logging_enabled` | No |
| Live transition matrix (binary) | Scorer KV Store `scoring_matrix_<scorer_sid>` | Key `matrix`, binary `serialize_kv` format | No |
| Matrix history & active JSON | FOS `processed/scoring/matrices/` | `index.json` + `<version>.json` + `matrix_latest.json` | No |
| Session labels | FOS `processed/scoring/labels.json` | JSON array with ETag / `If-Match` CAS retry | No |
| Scoring audit trail | Cloud `ops` log (`admin_audit` / `scoring_audit`) | Appended via `telemetry::write_log` | No |
| Prebuilt scorer Wasm | Embedded in `compute/cloud` (`include_bytes!`) | SHA-256 verified against `deployed_package_sha` | No |

#### `ScoringConfig` in `compute/cloud/src/admin/model.rs`

```rust
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScoringConfig {
    pub enabled: bool,
    pub scoring_service_id: String,
    pub scoring_service_name: String,
    pub scoring_domain: String,
    pub scoring_keys_store_id: String,
    pub scoring_config_store_id: String,
    pub scoring_matrix_store_id: String,
    pub enabled_at: String,
    pub deployed_package_sha: String,
    pub deployed_package_files_hash: String,
    pub deployed_vcl_sha: String,
    pub exclude_url_regex: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enforce_status_code: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub threshold: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matrix_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_retrain_at: Option<String>,
}
```

`Service` gains `#[serde(default, skip_serializing_if = "Option::is_none")] pub
scoring: Option<ScoringConfig>`. `service_value` exposes `scoring` directly
because it contains no secrets.

#### Embedded Wasm package

`compute/cloud` embeds `compute/scorer/pkg/session-scorer.tar.gz` at compile
time (`include_bytes!`), or reads it from an optional build-time path. This
guarantees `compute/cloud` can deploy the exact Wasm artifact whose SHA-256
matches `shipped_scorer_identity` without depending on external package hosting
or local disk at runtime.

- `compute/cloud` checks `deployed_package_sha` against the embedded SHA-256 to
  compute `scorer_drift` and `drift_detail` (`"wasm"`, `"vcl"`, or `"wasm+vcl"`).
- Note on package upload budget: `PUT /service/{id}/version/{v}/package` sends
  the `.tar.gz` payload as a single `ApiRequest` with `FOS_TIMEOUT` (60s)
  capped by `request_timeout` within its dedicated job step.

### 3. Cloud-compatible retrain and evaluation data path

`compute/cloud` (128 MiB heap, 45s step budget) does not scan multi-day request
Parquet files in Rust. Instead, training splits cleanly between the browser's
existing DuckDB-Wasm engine and `compute/cloud`'s Fastly/FOS writer:

1. **Browser (DuckDB-Wasm + `frontend/lib/cloud-engine/scoring-train.ts`)**:
   - Triggered when the admin clicks **Retrain** on `/admin/session-scoring`.
   - Scans `processed/request/` Parquet over `since_days` (default `7`, `1..=90`)
     using `ensureCloudRange` and existing `CloudBudgetError` guards.
   - Extracts the trace columns (`timestamp`, `ip`, `ua`, `url`, `method`,
     `status`, `referer`, `ttfb`, `country`, `asn`), sessionizes by `(ip, ua)`
     ordered by `timestamp`, filters short/fast sessions (`too_few_events`,
     `too_fast`), normalizes routes, and builds the transition matrix JSON
     (`version`: `YYYY-MM-DD-<suffix>` or explicit `version` override).
   - Fetches `GET /api/services/{id}/scoring/labels` and, when `n_good >= 5` and
     `n_bad >= 5`, evaluates ROC-AUC locally against the labeled sessions.
   - Sends the resulting `matrix` JSON + training `stats` + `auc_against_labels`
     to `POST /api/services/{id}/scoring/retrain`.
2. **Server (`compute/cloud` `POST /api/services/{id}/scoring/retrain`)**:
   - Validates the matrix JSON structure, vocabulary size bounds, and version
     string (`YYYY-MM-DD-...`).
   - Writes `processed/scoring/matrices/<version>.json` and
     `processed/scoring/matrix_latest.json` to FOS using `fos_access_key_ref` /
     `fos_secret_key_ref`, and updates `processed/scoring/matrices/index.json`
     (capped at the most recent 50 versions, pruning older JSONs if needed).
   - Serializes the matrix into the compact binary format expected by
     `compute/scorer/src/matrix.rs` and `PUT`s it to
     `/resources/stores/kv/{scoring_matrix_store_id}/keys/matrix` using
     `fastly_token_ref`.
   - Updates `svc:<sid>.scoring.matrix_version` and `last_retrain_at` via CAS,
     and records `matrix_retrained` in the scoring audit log.

**Evaluation & curves (`GET /scoring/evaluation`, `/evaluation/per-reason`,
`/curves`, `/threshold-preview`)**:
- `scoring-reads.ts` computes these in DuckDB-Wasm using the stored labels from
  `GET /api/services/{id}/scoring/labels` and the active matrix from
  `GET /api/services/{id}/scoring/matrix/active` (served from FOS via
  `compute/cloud`).

### 4. Provisioning, control-plane, and safe teardown flows

#### Preflight & entitlements

Before starting `scoring_enable`, `compute/cloud` checks:
1. Service exists, is owned by this install, `status != "torn_down"`, and no
   blocking job is in progress.
2. Logging service has an active VCL version and required scoring pragmas
   (`service_has_scoring_pragmas`, matching High-Scale's preflight check). If
   missing, returns `409` `missing_scoring_pragmas`.
3. Account entitlements (`compute`, `config_store`, `kv_store`) via Fastly
   `/enabled-products/v1/*`. Missing entitlements return `403` with machine
   `code` (`compute_not_enabled`, `config_store_not_enabled`,
   `kv_store_not_enabled`) and `manage.fastly.com` link.

#### `scoring_enable` Job (`POST /api/services/{id}/scoring/enable`)

Runs as a `Job` (`kind = "scoring_enable"`) stepped via
`POST /api/admin/jobs/{id}/step`:

| Step | Actions | Budget / Idempotency |
| :--- | :--- | :--- |
| `scorer_service` | Find or create `Session Scoring Service for <sid>` (`type: wasm`), add domain `fos-<sid_lower>-session-scorer.edgecompute.app`, add `placeholder_origin` backend. | Idempotent lookup by name before `POST /service`. |
| `scorer_stores` | Find or create `scoring_keys_<scorer_sid>`, `scoring_config_<scorer_sid>` ConfigStores and `scoring_matrix_<scorer_sid>` KV Store; seed `current_key_hex`, `request_secret` (also saved to Secret Store `scoring_request_secret.<sid>`), `debug_logging_enabled=0`, write-once `scoring_enabled_at`; link all 3 resources to scorer v1. | Idempotent store lookup by name; write-once `scoring_enabled_at`. |
| `scorer_deploy` | Upload embedded `session-scorer.tar.gz` to scorer v1 (or clone draft if v1 active), activate scorer version, and seed initial `matrix` key in `scoring_matrix_<scorer_sid>` KV Store (from FOS `matrix_latest.json` if present, else embedded default matrix). | Skips package upload if `deployed_package_sha` already matches. |
| `logging_reconcile` | Update `svc:<sid>.scoring` (`enabled: true`, resource IDs, SHAs) and merge the 9 canonical `edge_*` `custom_fields` into `service.log_fields` via CAS; run Cloud's VCL reconciler on the logging service to add `session_scorer` backend, `recv` + `enforce` VCL snippets, and updated log format, then activate. | Reuses `reconcile::reconcile_service` machinery; rolls back `svc:<sid>.scoring` if VCL activation fails before completion. |

#### Live control-plane endpoints (synchronous in `compute/cloud`)

Each fits well within a single Compute request (< 5 Fastly API calls):

- `GET /api/services/{id}/scoring/status` and `GET /api/services/{id}/scoring/config`:
  Reads `svc:<sid>.scoring` + live scorer active version / pragmas (cached in
  Core Cache / short TTL). Never returns secrets.
- `PUT /api/services/{id}/scoring/threshold`: Updates `svc:<sid>.scoring.threshold`
  in KV via CAS (advisory display threshold).
- `GET/PUT /api/services/{id}/scoring/enforce-threshold`: Reads/writes
  `enforce_threshold` in `scoring_config_<scorer_sid>` ConfigStore (`?confirm=true`).
- `GET/PUT /api/services/{id}/scoring/l2-enforce`: Reads/writes
  `l2_enforce_enabled` and `l2_enabled_at` in `scoring_config_<scorer_sid>`
  (`?confirm=true`).
- `GET/PUT /api/services/{id}/scoring/exclude-regex` + `POST .../validate`:
  Validates regex safely, updates `svc:<sid>.scoring.exclude_url_regex`, and
  clones/updates/activates the `Fastly Log Analytics - Session Scoring (recv)`
  snippet (`?confirm=true`).
- `GET/PUT /api/services/{id}/scoring/enforce-status-code`: Updates
  `svc:<sid>.scoring.enforce_status_code` and clones/updates/activates the
  `Fastly Log Analytics - Session Scoring (enforce)` snippet (`?confirm=true`).
- `POST /api/services/{id}/scoring/rotate-key`: Generates new 32-byte
  `aes_key_hex`, shifts `current_key_hex` → `previous_key_hex`, writes new
  `current_key_hex` in `scoring_keys_<scorer_sid>` (`?confirm=true`).
- `GET /api/services/{id}/scoring/matrix/versions` & `POST .../restore`:
  Lists `processed/scoring/matrices/index.json` from FOS; `restore` reads the
  selected version JSON from FOS and pushes it to `scoring_matrix_<scorer_sid>`
  KV Store (`?confirm=true`).
- `GET/POST/PATCH/DELETE /api/services/{id}/scoring/labels`: Reads/mutates
  `processed/scoring/labels.json` in FOS.
- `GET /api/services/{id}/scoring/audit`: Reads scoring audit entries from
  Cloud's `ops` logs.

#### Safe disable & teardown (`scoring_disable` / `scoring_teardown`)

Both `POST /api/services/{id}/scoring/disable` and
`POST /api/services/{id}/scoring/teardown` (plus `POST /api/provision/teardown`
when `service.scoring` is present) enforce strict **VCL-first deletion
ordering**:

```
Step 1: strip_logging_vcl (MUST succeed before Step 2)
  ├─ Clone active logging service version
  ├─ Delete `Fastly Log Analytics - Session Scoring (recv)` & `(enforce)` snippets
  ├─ Delete `session_scorer` backend
  ├─ For `disable`: remove the 9 scoring `custom_fields` & regenerate capture VCL + log format
  ├─ Validate & activate logging service version
  └─ ON FAILURE: abort job, leave Compute scorer + stores intact!

Step 2: delete_scorer_resources (only reached after Step 1 activates)
  ├─ Deactivate (if needed) and DELETE Compute service `scoring_service_id`
  ├─ DELETE ConfigStore `scoring_keys_<scorer_sid>`
  ├─ DELETE ConfigStore `scoring_config_<scorer_sid>`
  ├─ DELETE KV Store `scoring_matrix_<scorer_sid>`
  └─ Store deletion failures are recorded as non-fatal warnings in job result

Step 3: cleanup_state
  ├─ Clear `svc:<sid>.scoring` via CAS (for `disable`; full teardown removes `svc:<sid>`)
  ├─ Delete `scoring_request_secret.<sid>` from Secret Store (best-effort)
  ├─ Delete `processed/scoring/*` in FOS (always on `teardown`; on `disable` when `delete_matrices=true` or by default matching High-Scale)
  └─ Record `scoring_disabled` / `scoring_torn_down` in ops audit log
```

When `POST /api/provision/teardown` is called on a service with
`service.scoring.is_some()`:
- The existing `teardown` job runs `strip_logging_vcl` +
  `delete_scorer_resources` + `cleanup_state` **before** `remove_logging`,
  `delete_fos_keys`, and `delete_data`, ensuring `fastly_token_ref` and FOS
  keys are still available during scoring cleanup and no live traffic ever hits
  a deleted Compute scorer.
- Standalone `POST /api/services/{id}/scoring/teardown` requires
  `confirm_service_id` + `generation` (matching Cloud's `teardown` contract) and
  runs the same 3-step job without touching the logging endpoint or request/RUM
  data.

### 5. Frontend & `cloud-engine` integration

1. **`frontend/lib/cloud-engine/native-routes.ts`**: Register the new native
   Cloud scoring routes (`/api/services/{service_id}/scoring/status`, `config`,
   `enable`, `disable`, `teardown`, `retrain`, `threshold`,
   `enforce-threshold`, `l2-enforce`, `exclude-regex`, `exclude-regex/validate`,
   `enforce-status-code`, `rotate-key`, `matrix/versions`,
   `matrix/versions/restore`, `matrix/active`, `labels`, `labels/{label_id}`,
   `audit`).
2. **`frontend/lib/cloud-engine/scoring-reads.ts`**:
   - Remove the static `cloud_scoring_management_unsupported` stub for
     `/scoring/status` and `/scoring/config`; forward them to `compute/cloud`.
   - Keep DuckDB-Wasm Parquet scans for `CLOUD_SCORING_SCAN_ROUTES` and add
     DuckDB-Wasm evaluation/curve computation using FOS labels + active matrix.
   - Update `CLOUD_SCORING_CAPABILITIES` dynamically from `/scoring/status` (or
     enable all management/labels/evaluation/thresholds/matrix_versions/audit
     capabilities once Cloud scoring provisioning is active).
3. **`frontend/app/admin/session-scoring/page.tsx`**:
   - Replace the `isCloudMode()` branch that renders the read-only
     `CloudScoringOverview` with the full `SessionScoringPageContent` UI,
     adapting **Enable / Disable** and **Teardown** buttons to drive Cloud's
     job-polling flow (`runJob` -> `POST /api/admin/jobs/{id}/step`) and
     adapting **Retrain** to run browser DuckDB-Wasm training before posting to
     `/scoring/retrain`.
   - When `scoring.enabled === false` and historical `edge_score*` columns
     exist in Parquet, still render the historical analytics cards alongside the
     Enable Scoring panel.

---

## Security & Secret Guarantees

- **Zero browser Fastly credentials for routine ops**: Browser only holds a
  Cloud admin passkey session cookie. `fastly_token_ref` stays in Secret Store.
- **No secret persistence in KV or FOS**: `svc:<sid>`, `job:<id>`,
  `processed/scoring/*`, and `ops` logs never contain `token`, `request_secret`,
  or `aes_key_hex`.
- **Authenticated internal hops**: `compute/cloud` validates `require_admin`
  before brokering any Fastly API call or reading `fastly_token_ref`.
- **Scoped failure on missing token/scope**: If a customer's stored
  `fastly_token_ref` lacks Compute/KV/ConfigStore scopes, `scoring_enable`
  fails closed at preflight before creating any resources.

## Failure Modes & Recovery

| Failure | Behavior | Recovery |
| :--- | :--- | :--- |
| Missing Compute/KV/ConfigStore entitlement | Preflight returns `403` with `code` + Fastly manage URL; no resources created. | Enable product in Fastly and retry `scoring_enable`. |
| `scorer_stores` or `scorer_deploy` fails mid-enable | Job stops; partial Compute service/stores remain identifiable by deterministic name (`Session Scoring Service for <sid>`). | Retry job or run `scoring_teardown`; `scoring_enable` is idempotent and reuses existing named resources. |
| `logging_reconcile` fails during `scoring_enable` | Logging VCL is unchanged (draft not activated); `svc:<sid>.scoring.enabled` stays `false`. | Fix VCL conflict and retry; or run `scoring_teardown` to clean up Compute scorer. |
| `strip_logging_vcl` fails during `disable` / `teardown` | Job fails at `strip_logging_vcl`; Compute scorer and stores are **not** deleted. | Resolve logging VCL error (or supply fresh token) and retry `disable`/`teardown`. |
| Store deletion returns 404/409 during `delete_scorer_resources` | Recorded as warning on job completion; `svc:<sid>.scoring` is still cleared. | Operator can remove orphaned store manually in Fastly if needed. |
| Browser tab closes during `retrain` | No partial matrix is written to FOS or KV because `compute/cloud` only writes after receiving the complete trained matrix payload. | Re-run Retrain from UI. |
| Secret Store propagation delay on new `fastly_token_ref` | `put_secret` + `confirm_with_secret` handles Secret Store eventual consistency (already built into `compute/cloud/src/admin/mod.rs`). | Automatic retry within job step budget. |

## Verification Plan

1. **Rust unit & integration tests (`compute/cloud/tests/`)**:
   - `scoring_enable` job steps with mock Fastly API: entitlement checks,
     idempotent reuse, secret isolation (verify `svc:<sid>` and `job:<id>`
     contain no `token`, `request_secret`, or `aes_key_hex`), and rollback on
     `logging_reconcile` failure.
   - `scoring_disable` and `scoring_teardown` ordering test: inject VCL strip
     failure and assert **zero** `DELETE /service/{scorer_id}` or store delete
     calls are issued.
   - `POST /api/provision/teardown` with `scoring.enabled = true`: assert
     scoring VCL strip and scorer deletion run before logging/FOS teardown.
   - Route auth tests (`route_auth.rs`): verify non-admin / unauthenticated
     requests to all new `/api/services/{id}/scoring/*` routes return 401/403.
2. **TypeScript / Vitest tests (`frontend/lib/cloud-engine/`)**:
   - `native-routes.test` / `adapter.test`: verify all scoring control-plane
     routes route to `compute/cloud` with no `token` required in routine
     requests.
   - `scoring-train.test`: verify DuckDB-Wasm sessionization and matrix
     building produce the exact JSON schema expected by `compute/cloud` and
     `compute/scorer`, and respect `CloudBudgetError`.
3. **Combined live verification**:
   - Deploy and test both `flp-cloud` and High-Scale via `fdt` against a test
     service to verify end-to-end enable, retrain, threshold/L2 enforcement,
     key rotation, and teardown.

## Open Questions

1. **Wasm binary distribution in `compute/cloud`**: Embedding
   `session-scorer.tar.gz` via `include_bytes!` is simplest and guarantees
   version lockstep, but increases the `compute/cloud` Wasm package size by
   ~1–2 MB. Confirm `compute/cloud` stays comfortably under Fastly Compute's
   compressed package limit, or whether `compute/cloud` should read the scorer
   artifact from a versioned FOS prefix instead.
2. **Matrix retention on `disable`**: In High-Scale, `disable_scoring` deletes
   the FOS-published matrix (`_delete_scoring_matrix_from_fos`), whereas
   `labels` remain in Postgres. In Cloud, should `scoring_disable` keep
   `processed/scoring/labels.json` and/or `matrices/` in FOS by default (only
   deleting them on `scoring_teardown` or when `delete_data` is requested), so
   re-enabling scoring preserves historical labels and matrices?
3. **Browser vs server-side regex lint (`exclude-regex/validate`)**: High-Scale
   runs `falco` on the backend to lint the generated `recv` VCL snippet.
   `compute/cloud` cannot shell out to `falco`; should Cloud validate the regex
   syntax + VCL safety rules (length, no unescaped quotes/braces) in Rust
   without `falco`, relying on Fastly's `/service/{id}/version/{v}/validate`
   API during `PUT /exclude-regex`?
