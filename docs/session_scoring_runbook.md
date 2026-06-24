# Session Scoring — Operator Runbook

Day-2 operations for the session-scoring subsystem (v1.1.0). Aimed at the on-call engineer who already has the app deployed and a Fastly token in hand.

| Field | Value |
| :--- | :--- |
| **Subsystem version** | v1.1.0 |
| **Surface area** | `/admin/session-scoring` (UI) · `/api/services/{svc}/scoring/*` (API) |
| **Storage** | `backend/metadata.db` (audit log) · Fastly Object Storage (`scoring_matrix_history/{version}.json` version history) · Fastly **KV Store** (`scoring_matrix`, key `matrix` — the live L2 matrix the edge scorer reads at runtime) · Compute ConfigStore (`scoring_config`: `enforce_threshold`, `l2_enforce_enabled`, `l2_enabled_at`) |
| **Edge components** | 6 VCL snippets (recv / pass / fetch / deliver / miss / enforce) + scorer Wasm service |
| **Audit scope** | Per-host; not mirrored via `state_sync` |

> Reminder: rate-limiting is **out of scope** for v1.1.0. Session scoring observes and (optionally) blocks at the score-threshold level. When `fastly.ddos_detected` fires, Compute is bypassed entirely — the gate is upstream.

---

## Authentication

Admin and scoring endpoints are gated by the deployment's **admin trust model**
(loopback / reverse-proxy marker), **not** a bearer token. When an operation has
to call the **Fastly API**, supply the Fastly token as a **`token:` request
header** for the threshold / enforce / exclude-regex / enforce-status-code /
rotate-key mutations, or in the **JSON body** `{"token": "…"}` for
`enable` / `disable`. If omitted, the token falls back to the service's stored
`fastly_api_key`. An `Authorization: Bearer …` header is **ignored**.
`matrix-versions/{v}/restore` and the read-only GETs take no token (they resolve
it from config when needed).

---

## Enable / Disable

### Enable scoring on a service

**UI.** `/admin/session-scoring` → pick the service → **Enable**. Wait for the install log to settle on green.

**API.**

```bash
curl -sS -X POST \
  "$HOST/api/services/$SVC/scoring/enable" \
  -H "Content-Type: application/json" \
  -d "{\"token\": \"$TOKEN\"}"
```

The orchestrator installs the following on your behalf — all rolled back together if any step fails:

| Component | Where it lands |
| :--- | :--- |
| VCL snippets | The target VCL service — six snippets: `recv`, `pass`, `fetch`, `deliver`, `miss`, `enforce` |
| Custom log fields | Appended to the service's existing log format (does not displace your existing fields) |
| Scorer Wasm service | A separate Compute service in your account; receives the scoring requests from VCL |
| ConfigStores | `scoring_config` (enforce threshold + L2 opt-in) + `scoring_keys` (AES current/previous slots) |
| Cookie keys | Generated and seeded into the current slot of `scoring_keys` |

Confirm with `GET /api/services/$SVC/scoring/status` — `enabled: true`, snippets installed, scorer service ID populated.

### Disable scoring cleanly

**UI.** `/admin/session-scoring` → service row → **Disable** → confirm.

**API.**

```bash
curl -sS -X POST \
  "$HOST/api/services/$SVC/scoring/disable" \
  -H "Content-Type: application/json" \
  -d "{\"token\": \"$TOKEN\"}"
```

What gets torn down: VCL snippets removed, custom fields unregistered, scorer Wasm service deactivated, ConfigStore entries cleared.

What is **preserved per-host** (intentionally):

- The `scoring_audit` table for this service in `metadata.db` — every prior mutation stays queryable.
- Matrix history under `scoring_matrix_history/` in the FOS bucket — re-enabling later can restore a prior version.

If you need a hard wipe, delete the `scoring_audit` rows manually and remove the `scoring_matrix_history/{*}.json` objects from the bucket. Do this only when you're sure no compliance or forensics need them.

---

## Operate

### Rotate the AES cookie key

**When.** On a regular cadence (quarterly is reasonable) or immediately on suspected compromise of a host that ran the admin UI.

**How.**

```bash
curl -sS -X POST \
  "$HOST/api/services/$SVC/scoring/rotate-key" \
  -H "token: $TOKEN"
```

Or in the UI: `/admin/session-scoring` → service detail → **Rotate AES key**.

**What happens.** The current key moves to the **previous** slot and a freshly generated key takes the **current** slot. The scorer accepts cookies signed by **either** key for one grace cycle, so in-flight sessions don't see a wave of tampered-cookie events at the moment of rotation.

**IMPORTANT — do not double-rotate.** Rotating twice within seconds discards the original key (it cascades out of the previous slot before any session can be re-issued). Cookies signed under that original key will then be flagged as tampered. Always wait — at minimum long enough for one full request/response round-trip on your slowest sessions, comfortably one full minute — between rotations. The audit log records every rotation with its timestamp; check it before rotating again.

### Roll back a bad matrix

**When.** A retrain hurt AUC, the score distribution shifted in a way that doesn't match recent traffic, or per-reason metrics show a rule degrading.

**How.** `/admin/session-scoring` → **Matrix history** tab → find the target version → **Restore** → confirm.

API equivalent:

```bash
curl -sS -X POST \
  "$HOST/api/services/$SVC/scoring/matrix-versions/$VERSION/restore?confirm=true"
```

**What happens immediately:**

- A pre-restore snapshot is saved (so the restore itself is reversible — restoring the rolled-back version brings you back).
- Admin AUC and the dashboard score distribution reflect the restored matrix on the next refresh.
- The restored matrix is **automatically pushed to the `scoring_matrix` KV Store**, so the live edge scorer rolls back too — **no Wasm redeploy**.
- The audit log records the restore with the source and target versions (and whether the KV push succeeded).

**Edge propagation — no redeploy, but not instant.** The matrix lives in a KV Store the scorer reads at runtime, *not* embedded in the Wasm, so a restore re-flashes the edge by itself. The change takes effect **as Compute instances recycle (minutes)** — each instance reads the matrix on cold start. The restore API response tells you which case you're in:

- `matrix_kv_written: true` → `deploy_hint: "Live: restored matrix pushed to the scoring_matrix KV Store — the edge scorer rolls back as Compute instances recycle (minutes), no redeploy."` You're done; just wait out the recycle window.
- `matrix_kv_written: false` → the backend AUC/evaluation reflect the restore, but the live scorer was **not** updated (no KV store id or no Fastly token available). Re-run **Enable** (or supply a token) to seed the KV Store — otherwise the edge keeps serving the old matrix.

Confirm in `/admin/session-scoring` that the scorer's `MATRIX_LOAD_FAIL_COUNT` stays 0 after the recycle window — that's the signal the KV matrix is serving cleanly end-to-end.

### Emergency disable of enforcement

Enforcement is the part that returns `429`s. Scoring (the score appearing in logs) is independent of enforcement — you can run with scoring on and enforcement off indefinitely.

**Fastest path (API).**

```bash
# Verify the current enforced threshold
curl -sS "$HOST/api/services/$SVC/scoring/enforce-threshold" \
  -H "token: $TOKEN"

# Clear it
curl -sS -X PUT \
  "$HOST/api/services/$SVC/scoring/enforce-threshold?confirm=true" \
  -H "token: $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"threshold": null}'
```

**UI path.** `/admin/session-scoring` → service detail → **ThresholdSlider** → **Disable** (the button is labeled this way only when enforcement is currently live).

**Propagation.** The change is written to the Compute `enforce_threshold` ConfigStore key. Effective within seconds — the scorer reads the key on every request. No Wasm redeploy needed.

**What it does and doesn't do.**

- Disables 429s. The scorer stops emitting `X-Edge-Score-Enforce=1`, the VCL `enforce` snippet has nothing to act on, no requests get restarted with a 429.
- **Does not** disable scoring. Scores keep appearing in logs. The compliance/dashboard views continue to update. You retain visibility while the false-positive trigger is investigated.

**Total kill switch.** If you need scoring itself off (not just enforcement), use the disable endpoint from the previous section: `POST /api/services/$SVC/scoring/disable`. That removes the VCL snippets and stops the scorer from running at all.

### Change the enforce response code

The enforce snippet defaults to returning `HTTP 429 Too Many Requests` for flagged requests. Operators can override this per-service to any 4xx/5xx code — common picks are `403` (policy block), `451` (legal), and `503` (degraded). The status code is baked into VCL at deploy time, so a change does a focused snippet swap (~5–10s end-to-end).

**UI path.** `/admin/session-scoring` → service detail → **ThresholdSlider** → **Enforce response code** selector (next to the threshold). Picking a new code opens a confirm dialog before publishing.

**API.**

```bash
# Read the current code (returns default 429 when never overridden)
curl -sS "$HOST/api/services/$SVC/scoring/enforce-status-code" \
  -H "token: $TOKEN"

# Set a new code
curl -sS -X PUT \
  "$HOST/api/services/$SVC/scoring/enforce-status-code?confirm=true" \
  -H "token: $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"status_code": 403}'

# Reset to default (429)
curl -sS -X PUT \
  "$HOST/api/services/$SVC/scoring/enforce-status-code?confirm=true" \
  -H "token: $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"status_code": null}'
```

**Validation.** Backend rejects anything outside 400–599. The reason phrase is auto-mapped to the HTTP standard for known codes (403→Forbidden, 451→Unavailable For Legal Reasons, 503→Service Unavailable, …) and falls back to `Blocked` for unmapped codes. Audit-logged as `scoring_enforce_status_code_changed`.

**Note.** Disabling enforcement (previous section) is unaffected by the response code — flipping enforcement off / on does not change the configured code, and changing the code does not flip enforcement.

### Read the audit log

**UI.** `/admin/session-scoring` → **Audit** tab. Filterable by event type and time window.

**API.**

```bash
curl -sS "$HOST/api/services/$SVC/scoring/audit?limit=200" \
  -H "token: $TOKEN"
```

**What's recorded.** Every mutation: enable, disable, threshold commit/clear, enforcement set/cleared, retrain, key rotation, matrix restore. Each row has the actor, timestamp, and a structured payload describing the before/after state.

**Per-host scope.** The audit log lives in this host's `metadata.db` and is **not** mirrored via `state_sync`. If the same logical service is operated from multiple admin hosts (e.g. a primary and a hot-standby), each host has its own independent audit log. For a complete history, query each host and merge by timestamp.

---

## Diagnose

### "AUC dropped after the last retrain."

1. `/admin/session-scoring` → **Per-reason AUC** card. Look for the rule whose AUC dropped — that's usually a single contributor driving the aggregate.
2. Confirm against the Matrix history tab: the version timestamp lines up with the retrain in the audit log.
3. If the regression is real and not a data artifact, roll back to the prior matrix (see *Roll back a bad matrix*) — the restore auto-pushes to the `scoring_matrix` KV Store, so the edge rolls back as Compute instances recycle (no Wasm redeploy).

### "Enforcement is 429-ing real users."

1. Open ThresholdSlider — the counterfactual preview shows what the **Precision %** and block volume would be at every candidate threshold against the recent score distribution.
2. If a higher threshold preserves true-positive coverage at acceptable precision: commit the new threshold via the slider.
3. If no threshold looks acceptable: disable enforcement entirely (see *Emergency disable of enforcement*). Scoring stays on for visibility while you dig in.

### "Cookie compliance shows a lot of tampered cookies."

Most common causes, in order:

1. **A double rotation** within the grace window (see the *IMPORTANT* note in *Rotate the AES cookie key*). Correlate the spike timestamp against `rotate-key` entries in the audit log.
2. **A replay or tampering attack** — a real bot population trying to forge cookies. Cross-reference with the dashboard's top-flagged-sessions list.
3. **A misconfigured upstream cache** stripping the `Set-Cookie` on first response (rarer, but causes the same symptom).

The fix for cause 1 is patience — wait one grace cycle and the noise subsides. The fix for cause 2 is enforcement (if it isn't already on). The fix for cause 3 is a cache config audit.

### "What does the scorer think it's doing?"

The Rust scorer keeps a set of `AtomicU64` counters and flushes them via `dbg_log` every 1000 requests:

- `tampered` — cookies that failed AES verification.
- `replayed` — sustained-replay cookies dropped to the no-state baseline.
- `enforce_block` — requests that emitted `X-Edge-Score-Enforce=1`.
- `matrix_fail` — matrix lookup failures (should be zero in steady state).
- `keys_fail` — key-load fail-opens (missing/unreadable `scoring_keys` store → `internal-error-keys`; should be zero in steady state).
- `cookie_encode_fail` — rotated-cookie re-encode failures (the `Set-Cookie` is dropped; should be zero).
- `requests` — total requests processed (the fail-open denominator).

In the backend's ingested logs, grep for the emitted line:

```
metrics: tampered=... replayed=... enforce_block=... matrix_fail=... keys_fail=... cookie_encode_fail=... requests=...
```

Rates are easier to reason about than absolute counts — divide each by the delta in `REQUEST_COUNT` between two flushes to get per-request rates.

If `MATRIX_LOAD_FAIL_COUNT` is non-zero, the scorer can't read the matrix from the `scoring_matrix` KV Store — the resource-link or `matrix` key is broken or the object is unreadable. Re-push the matrix (a **retrain** or a **matrix restore** both write to KV; or re-run **Enable** to re-seed the store) rather than redeploying the Wasm — the code deploy doesn't touch the matrix. If `TAMPERED_COOKIE_COUNT / REQUEST_COUNT` exceeds the baseline you've established for this service, run the cookie-compliance diagnosis above.

---

## Reference

- **Endpoints.** Full schema is at `/docs` (FastAPI Swagger UI on the running backend). Quick index:

  | Method | Path | Purpose |
  | :--- | :--- | :--- |
  | POST | `/api/services/{svc}/scoring/enable` | Provision scoring on a service |
  | POST | `/api/services/{svc}/scoring/disable` | Tear down scoring (audit preserved) |
  | GET | `/api/services/{svc}/scoring/status` | Installation + health snapshot |
  | GET | `/api/services/{svc}/scoring/labels` | Score labels (good / suspicious / bad bands) |
  | GET | `/api/services/{svc}/scoring/top-flagged` | Highest-scoring sessions in window |
  | GET | `/api/services/{svc}/scoring/score-distribution` | Histogram for ThresholdSlider |
  | GET | `/api/services/{svc}/scoring/compliance-breakdown` | Cookie compliance counters |
  | GET / PUT | `/api/services/{svc}/scoring/threshold` | Commit-style threshold |
  | GET | `/api/services/{svc}/scoring/threshold-preview` | Counterfactual preview |
  | POST | `/api/services/{svc}/scoring/retrain` | Recompute matrix from recent traffic |
  | GET | `/api/services/{svc}/scoring/dashboard` | Aggregated dashboard payload |
  | GET | `/api/services/{svc}/scoring/evaluation/per-reason` | Per-rule AUC |
  | GET | `/api/services/{svc}/scoring/audit` | Audit log |
  | POST | `/api/services/{svc}/scoring/rotate-key` | Rotate AES cookie key |
  | GET | `/api/services/{svc}/scoring/matrix-versions` | List matrix snapshots |
  | POST | `/api/services/{svc}/scoring/matrix-versions/{v}/restore` | Restore a matrix version |
  | GET / PUT | `/api/services/{svc}/scoring/enforce-threshold` | Live enforcement value (ConfigStore-backed) |
  | GET / PUT | `/api/services/{svc}/scoring/enforce-status-code` | Enforce response code (default 429) |
  | GET / PUT | `/api/services/{svc}/scoring/l2-enforce` | L2 enforcement opt-in + fade-in status |
  | GET / PUT / POST | `/api/services/{svc}/scoring/exclude-regex` | URL exclusion regex for the recv snippet (`/validate` dry-runs it) |

- **Custom log fields** added by enable: see `docs/features.md` for the full schema and the field-group toggles.
- **Architecture notes.** L1 (cookie + timing) + L2 (PageRank transition matrix) → 0–100 score. AES-encrypted cookie state with rotating `sid`; 30-minute idle timeout; 24-hour hard cap. See `backend/scoring/scorer.py` + `compute/scorer/src/scorer.rs` for the implementation.
- **L2 enforcement (operator opt-in).** Layer 2's transition-matrix sub-score (`edge_score_l2`) is computed and logged on every request, but it contributes **nothing** to the enforced score until an operator explicitly opts in — there is no automatic, time-based ramp. Turn it on per-service with the **Enforce L2** switch on `/admin/session-scoring` (or `PUT /api/services/{svc}/scoring/l2-enforce?confirm=true` with `{"enabled": true}`). On opt-in the scorer stamps an `l2_enabled_at` anchor and L2's weight fades in linearly from 0 to full over **3 days** (so a still-thin matrix can't over-flag the moment you flip it); disabling returns L2 to observe-only, and a later re-enable restarts the 3-day fade-in. Both `l2_enforce_enabled` and `l2_enabled_at` live in the `scoring_config` ConfigStore and are derived server-side — never from client headers. If you opt a service in while threshold enforcement is on, re-check the threshold before you flip the switch: sessions can score higher once L2 starts counting.
- **Security headers.** The `recv` snippet unsets eight client-controllable `X-Edge-*` headers before any scoring logic runs — clients cannot spoof scores or compliance state.
- **DDoS boundary.** `fastly.ddos_detected` bypasses the Compute scorer entirely; under attack, Fastly's upstream gate handles it.

---

*Last revised for v1.1.0. When the API surface changes, update the endpoint table above — operators copy-paste from it, so stale entries cost real time.*
