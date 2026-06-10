# ADR-11 — Secret Rotation Policy

**Status:** Accepted (2026-06-10)
**Decided by:** v2.0 cleanup retrospective ([pending-docs/velocity_improvements.md](../../pending-docs/velocity_improvements.md) Tier 2)

## 1. Context & Motivation

The project handles five categories of secret with five different lifecycles, only one of which has a documented rotation procedure:

| Secret | Where it lives | Today's rotation story |
|---|---|---|
| **Fastly API key** (`FASTLY_API_KEY`) | env var → [`backend/core/settings.py:155-157`](../../backend/core/settings.py) | No documented procedure |
| **FOS access key / secret** | per-service config | Generated at provision; no rotation path |
| **CDN secret** (`cdn_secret`) | per-service config, embedded in `x-fastly-key` header | Regenerated only on explicit provisioning re-run |
| **Share / analyst session passcode** | per-invite, Argon2id-hashed in `share_db` | Per-login upgrade from scrypt → argon2id; no time-based rotation |
| **AES-256 cookie key** (session scoring) | Fastly ConfigStore, two-slot grace window | **Documented runbook** ([docs/session_scoring_runbook.md](../../docs/session_scoring_runbook.md)) — the model to generalize |

The AES rotation runbook works. It encodes a real operational pattern — current key in slot A, previous key in slot B for one full session-idle timeout window — and the runbook explicitly warns against double-rotating within that window (because the second rotation evicts the original key before in-flight sessions can re-issue). That same shape applies to most of the other secrets in the table, but nobody has written it down.

This ADR generalises the grace-window pattern, names the secrets we expect to rotate, and codifies what to do when one is suspected leaked. It does NOT mandate a rotation cadence we don't actually run — for a solo-dev project, "rotate when there's a reason to" is more honest than "rotate every 90 days" with no enforcement.

## 2. Decision

### 2.1 Secret inventory + rotation cadence

| Secret | Trigger for rotation | Mechanism | Grace window |
|---|---|---|---|
| Fastly API key | Suspected leak; departing operator with access; never on a fixed schedule | Manual: regenerate in Fastly UI, update env, redeploy. New key takes over immediately. | None — Fastly API keys are independent; old + new can both be valid until old is revoked |
| FOS access key / secret | Suspected leak; AWS-side rotation policy if applicable; new bucket | Regenerate via FOS console; PATCH service config; redeploy backend (env doesn't pick up live) | Both keys valid in parallel during the redeploy window |
| CDN secret | Suspected leak; on FOS bucket reprovisioning | Re-run `/api/admin/services/{id}/regenerate_cdn_secret` endpoint; deploy backend (clients re-read on next bootstrap) | None — single-secret per service; brief 4xx window during redeploy |
| Share passcode | User-initiated; on per-invite expiry; no time-based rotation | Per-login opportunistic upgrade scrypt → argon2id via `needs_rehash` ([backend/core/share_db/passcode.py:117-138](../../backend/core/share_db/passcode.py)). New invites get OWASP-2026 argon2id params. | N/A — historic hashes verify until the user logs in or the invite is revoked |
| AES cookie key | Quarterly, suspected leak, or on operator transition | [docs/session_scoring_runbook.md:68-84](../../docs/session_scoring_runbook.md) two-slot rotation; new key in slot A, old in slot B | Two-slot ConfigStore. Wait ≥ session-idle-timeout (default 30 min) before second rotation |

### 2.2 The grace-window pattern (the AES model, generalised)

Use this pattern when a secret signs/encrypts material that needs to remain readable across a rotation. Concretely: anywhere a stored token, cookie, or signed artifact uses the secret.

1. **Generate** the new secret offline (or via the rotation endpoint).
2. **Stage** it as "primary" in the secret store, keep the previous secret as "secondary."
3. **Issue** new artifacts with primary; **verify** incoming artifacts against (primary OR secondary) for one full lifetime of the artifact.
4. **Demote** secondary after that window. Optional: keep secondary archived for forensic decrypt of historical artifacts.

The AES rotation endpoint ([backend/routers/session_scoring_admin.py:857-925](../../backend/routers/session_scoring_admin.py)) implements this. New rotation-bearing secrets follow the same shape.

**Do NOT** rotate twice within the grace window. The runbook IMPORTANT note exists because we've seen the consequence — every cookie signed under the original key becomes tampered-looking the moment the second rotation evicts the previous slot.

### 2.3 Incident response when a secret is suspected leaked

**Step 1 — Decide blast radius (5 min).** Which secret? Where could it have leaked from (commit, log, browser history, Slack)? What's signed/encrypted with it that someone could now forge?

**Step 2 — Revoke at the source if possible (15 min).** Fastly API keys: revoke in Fastly UI. FOS access keys: revoke at the IAM source. CDN secret / AES key: rotate per §2.2. Passcodes: revoke the invite via `/api/admin/share/invites/{id}/revoke`.

**Step 3 — Audit what touched the secret (rolling).** Backend has `share_audit` table for analyst session events ([backend/utils/remote_access.py:721-727](../../backend/utils/remote_access.py)). Check `audit_log` and `cron_runs` for the relevant time window. There is no per-secret access log today; if the leak surface is unknown, treat all of the secret's usage window as potentially compromised.

**Step 4 — Force re-validation of clients.** Restart the backend if the secret is env-loaded (Fastly API key, FOS keys). For analyst sessions: the next request will fail fingerprint validation if the key changed; user logs in again.

**Step 5 — Write a session note in [pending-docs/](../../pending-docs/).** Date, secret category, what was rotated, what was checked. The 2026-06-10 OTel-spam note is the format.

### 2.4 Storage hygiene (what NOT to do)

These are operational rules with code-level enforcement where possible:

- **Never put a secret in a URL query parameter.** It leaks to browser history, Referer headers, intermediaries, server access logs. The 2026-06 CDN-secret incident moved `cdn_secret` from query param to `x-fastly-key` header ([backend/routers/admin.py:439-444](../../backend/routers/admin.py)).
- **Never log a secret value.** Structlog redactor exists implicitly via `_SECRET_KEYS` masking in admin status responses; don't bypass.
- **Never commit a secret.** Pre-commit's `gitleaks` hook ([.pre-commit-config.yaml](../../.pre-commit-config.yaml)) is the gate; `.gitleaks.toml` allows tracked fixtures and Rust lockfile checksums. New legitimate placeholders need a `#gitleaks:allow` inline comment + entry in `.gitleaks.toml`.
- **Never email a secret.** Out-of-band delivery (1Password share, Slack DM with retention < 24h, in-person) only.

### 2.5 What "rotation" means at this scale

This is a solo-dev project; we are not running a SOC2 rotation cadence. The decision is:

- **Time-based rotation: NO** for any secret without a documented event-driven trigger above. Rotating-because-the-calendar-says-so on a solo project means the operator forgets to update the runbook on schedule and confidence drops.
- **Event-driven rotation: YES** for the triggers in §2.1. The grace-window pattern is the reusable machinery.
- **Cryptographic key strength: YES** — Argon2id with OWASP 2026 params, AES-256, never roll our own.

If/when SOC2 (or equivalent compliance) becomes a real requirement, replace this section with a real cadence. Until then, this is the honest answer.

## 3. Out of Scope

- **Frontend credential storage.** Tokens in localStorage, env-loaded API URLs, etc. live in the frontend codebase.
- **TLS certificate rotation** for production domains — Fastly-managed, not an application secret.
- **Database authentication** (DuckDB, SQLite) — local, ephemeral, not an API secret.
- **External breach-list integration** (HIBP k-anonymity). The hook exists ([backend/core/share_db/passcode.py:144](../../backend/core/share_db/passcode.py)) but the external service integration is its own concern.
- **Compute@Edge / VCL secrets** beyond what ConfigStore provisioning manages. The edge scorer's secret management is owned by the Compute service deployment, not this backend.
- **Compliance audit retention.** Audit log retention/export for compliance purposes is a separate problem; this ADR covers the rotation machinery, not the audit trail policy.

## 4. Failure Modes & Recovery

| Scenario | Behavior |
|---|---|
| AES key rotated twice within grace window | Every cookie signed under the original key flags as tampered → forced re-login for all active sessions. Recovery: wait out the session-idle-timeout (~30 min); affected users re-authenticate. Document in a session note. |
| Fastly API key revoked without backend env update | Backend's Fastly API calls start failing 401. Stats / provisioning ops break. Recovery: update env, redeploy. Surface via `/api/admin/health-snapshot` if it doesn't already (TODO). |
| CDN secret regenerated without service config save | New secret is in memory; restart loses it; clients calling with old secret get 4xx. Recovery: re-run the regenerate endpoint or POST the new secret via service-update. |
| Passcode hash format upgraded mid-flight | `verify_passcode` checks argon2id first, falls through to scrypt for legacy hashes. Old hashes keep working; new ones use argon2id. No active recovery needed. |
| Share session host suspected compromised | No documented incident playbook today. Manual procedure: revoke all invites for the affected service via admin endpoint; force all analysts to re-login. Add to this ADR if it happens. |
| Secret accidentally committed | gitleaks fails the commit. Recovery: rewrite history (`git rebase -i` + force-push if branch is private) AND rotate the secret per §2.3 because git history is not a security boundary. |
| Secret leaked in a log line | Hard to detect after the fact. Recovery per §2.3 — assume the secret is public, rotate immediately, audit what's been accessed under it. |

## 5. Verification

This ADR succeeds if:

- A secret-leak incident in the next 12 months follows §2.3 step-by-step instead of getting improvised.
- A new operator can read the inventory + grace-window pattern and execute an AES key rotation without re-reading the source code.
- No secret gets committed to the repo — gitleaks remains green on `main`.
- The next time we add a new secret category (e.g., for an external service integration), it shows up in §2.1 as part of the same PR.

It fails if a secret rotation incident requires improvised playbook-writing in the moment, or if a new secret category lands without an entry in §2.1.

## 6. Rollback

The secret machinery (Argon2id hashing, AES rotation endpoint, gitleaks pre-commit, env-var loading) is load-bearing security infrastructure and cannot be rolled back without re-introducing the vulnerabilities they prevent.

Rolling back this ADR means deleting the doc; the code stays. If we adopt SOC2 or similar compliance, replace this ADR with a cadence-driven version that the auditor expects.
