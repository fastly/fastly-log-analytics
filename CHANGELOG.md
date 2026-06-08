# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 2026-06-03

Edge session scoring. Every request is classified in real-time at the edge by a Fastly Compute service that runs an L1 (cookie compliance + timing rules) + L2 (PageRank-trained transition matrix) scorer, returning a combined 0-100 score that lands in DuckDB for analyst review. Operators can label sessions, watch live ROC-AUC, retrain the matrix, roll back to a prior matrix, rotate the AES cookie key, and push a hard enforcement threshold that rejects flagged requests at the edge with an operator-chosen HTTP status code (default 429).

### Highlights

- **Edge scoring** — Fastly Compute scorer + 6-snippet VCL preflight pattern (recv/pass/fetch/deliver/miss/enforce), AES-GCM-encrypted session cookie carrying rotating sid + transition state, `fastly.ddos_detected` gate so Compute is bypassed under L7 attack.
- **Admin UI** at `/admin/session-scoring` — StatusPanel with live AUC against accumulated labels, ScoringHealthCard with fire rate / score distribution / top reasons / matrix-staleness alert, ThresholdSlider with counterfactual flag/pass preview + precision/recall + commit-threshold persistence, RocPrCurves with ROC + Precision-Recall plots, TopFlaggedTable + LabelsTab with click-to-view-events per sid, RetrainButton (DuckDB traces → train.py → publish matrix to FOS), SinceHoursPicker driving all six cards on one shared time window.
- **Labels CRUD** — POST/PATCH/DELETE per-sid labels (good/bad/neutral) feed `evaluate_from_persisted_scores` to compute live ROC-AUC. Min-samples gate (≥3 per class) prevents noisy display.
- **ROC + PR curves** + per-reason AUC breakdown (split by L1/L2 rule: cookie-missing, impossibly-fast, robotic-consistency, rare-transition, low-transition-prob).
- **Composite `/scoring/dashboard`** endpoint collapses the 8 per-card requests into one in-flight-collapsed payload; the existing per-card endpoints stay mounted for back-compat.
- **`edge_score_reason` virtual field** — CSV-split via DuckDB `unnest(string_split(...))`, top-N cards + click-to-filter same as NGWAF signals.
- **FOS matrix persistence** — `enable_scoring` publishes the trained matrix to FOS; backend auto-fetches on startup (no more per-host scp).
- **Matrix version history + rollback** — every publish snapshots the prior matrix to `iceberg/meta/scoring_matrix_history/{version}.json`; new `/scoring/matrix-versions` lists them and `/scoring/matrix-versions/{v}/restore?confirm=true` copies a historical matrix back. AUC reflects the rollback immediately; Wasm at edge keeps the embedded matrix until `deploy_wasm.sh` re-runs (deploy_hint surfaced).
- **Threshold enforcement (live blocking)** — operator commits a threshold, scorer reads it from `scoring_config` ConfigStore, emits `X-Edge-Score-Enforce: 1` when score≥threshold, the new `Session Scoring - Enforce` VCL snippet rejects those requests on the post-scoring restart. Effective at the edge within seconds. Confirm-dialog-gated PUT endpoint + LIVE warning chip in the slider UI. The response code defaults to 429 (Too Many Requests) and is operator-overridable per-service via a new `Enforce response code` selector (403 / 429 / 451 / 503; backend accepts any 4xx/5xx) — picks land via a focused `update_enforce_status_code` orchestrator that swaps only the enforce snippet (~5–10s end-to-end vs. the full enable_scoring flow). Audit-logged as `scoring_enforce_status_code_changed`.
- **URL exclusion regex override** — operator-tunable per-service regex for "which URLs bypass the scorer". Defaults to the built-in static-asset extension list; the new `ExcludeRegexCard` on the Session Scoring page accepts a custom regex (e.g. exclude `/healthz`, exclude entire path prefixes, scope scoring to specific traffic). The PUT endpoint validates input through three layers before any VCL ships: (1) input policy — length cap, no quote / control chars, must compile under Python's `re`; (2) [falco](https://github.com/ysugimoto/falco) static analysis on the assembled recv snippet (catches regex+VCL composition errors that slip past Python's compiler); (3) Fastly's own VCL compiler at activate time. A focused `update_recv_exclusion_regex` orchestrator clones the active version, swaps only the recv snippet, and activates — ~5–15s end-to-end vs. the full enable_scoring flow. Confirm-dialog-gated. Audit-logged as `scoring_exclude_regex_changed`. Falco shipped in the backend Docker image; production sets `SCORING_REQUIRE_FALCO=1` so a missing binary fails closed instead of degrading to input-policy-only.
- **AES key rotation** — `POST /scoring/rotate-key` mints a fresh 32-byte key, moves the prior to `previous_key_hex` (grace slot — Rust cookie codec falls back to it so in-flight cookies keep decoding through one rotation cycle).
- **Cookie lifecycle bounds** — `SESSION_IDLE_EXPIRE_S` (30 min) + `SESSION_HARD_CAP_S` (24h) in the Rust scorer mint a fresh sid when either threshold is exceeded. Stolen cookies can't replay beyond their window; long-running sessions stop biasing the L1 variance estimator.
- **Per-reason AUC breakdown UI** — `PerReasonAucCard` renders AUC split by which L1/L2 rule fired (cookie-missing, impossibly-fast, robotic-consistency, rare-transition, low-transition-prob).
- **Operator audit log** — new `scoring_audit` table + `/scoring/audit` endpoint records every scoring_enabled, scoring_disabled, threshold_committed/cleared/enforced, matrix_retrained/restored, key_rotated event with actor + timestamp + details. Per-host, never mirrored via state_sync.

### Reliability

- **Cron-progress reliability** — `end_progress` auto-emits `done` when the last event isn't terminal; `list_active_runs` triple-guards (last-event filter + 5-min staleness + DB-status cross-check via `get_cron_run_status`); `reap_zombie_runs` called from every cron-tick cleanup. Fixed a production incident where 382 stale "sync" entries piled up on the System Health card.
- **state_sync merge guards** — `import_admin_state` no longer overwrites scoring `custom_fields` with stale FOS payloads (root cause of a production data-loss incident); sibling fixes in `cli.handle_update_logs`, `provision.write_service_config`, and `api_service_log_fields_set` close every "remote-overwrites-code-managed-state" path.
- **Defense-in-depth** — `enable_scoring` rollback + `disable_scoring` final-save reload cfg right before writing to close the 30-120s race window where concurrent writers got clobbered.
- **Per-key in-flight collapse** in `_cached` so the dashboard's 8-card mount no longer queues queries behind one global lock.

### Performance

Structural:

- **DuckDB connection pool** (`backend/core/duckdb_pool.py`) replaces per-request connection setup; eliminates the per-request DuckDB initialisation cost on hot paths.
- **Hourly Top-N rollup pipeline** (`backend/core/rollups.py` + `scripts/backfill_rollups.py`) precomputes the dashboard's most-asked aggregates; cold-load dashboard scans drop from seconds to tens of ms.
- **Bounded cache primitive** (`backend/utils/bounded_cache.py`, 13-test `tests/utils/test_bounded_cache.py`) replaces several previously-unbounded dict caches across the request path (also referenced under Security → `_StaticAssetLimiter` and the analytics cache in `session_scoring._cached`).

Tuning:

- `security/top-bots` consolidated UA + NGWAF onto one temp table (was 2 independent Iceberg scans per dashboard mount).
- `dashboard/raw` uses `get_source_extent` for cached steady-state extent.
- `usage/prefill` cached-status fast path skips DuckDB hop when the sync cron has populated it.
- `get_enriched_services` 60s TTL cache on the recursive cache-dir `scandir` (was 200-1500ms per `/api/bootstrap`).
- `loading.tsx` Suspense skeletons + dynamic imports (LabelsTab, ChoroplethMap) cut admin-page click lag.

### Cleanup

- Dropped dead `@daypicker/react` dep + dead `frontend/components/ui/calendar.tsx`.
- Collapsed 7-site `cleanup_progress + reap` boilerplate into `cleanup_progress_and_reap()` helper.
- Refactored `security.py`'s ad-hoc temp-table to use the existing `QueryRunner.temp_table()` context manager.
- Narrowed `get_cron_run_status` exception scope to `sqlite3.Error` with DEBUG log so future triage isn't flying blind.

### Security

Capability-focused hardening across the FastAPI backend, Fastly VCL, Next.js frontend, and Rust scorer. All changes deployed and verified.

- **Trust-boundary normalisation**:
  - uvicorn runs with `--proxy-headers --forwarded-allow-ips=127.0.0.1` so `request.client.host` is the real client IP via Caddy's authoritative XFF rewrite.
  - `is_request_remote()` reads `request.client.host` instead of the forgeable Host header; in-app leftmost-XFF parsing is gone.
  - Caddyfile gates `Fastly-Client-IP → X-Forwarded-For` rewrite on `remote_ip` matching Fastly edge ranges. Startup assertion on `TRUSTED_PROXY_IPS` / `UVICORN_FORWARDED_ALLOW_IPS` + integration test prevent silent regression.
  - Next.js `/admin` middleware gates on the Caddy-injected `X-Proxied-By-Caddy: true` marker instead of the forgeable Host header.
- **Destructive-op auth**:
  - `/api/provision/teardown` validates a caller-supplied Fastly token via `/tokens/self` for the `global` scope before any destructive op; never falls back to server-stored credentials. Frontend TeardownDialog prompts admin for the token.
  - `/api/provision/ngwaf-workspaces` token-gated (constant-time stored-key match OR validated `global`-scope token); NGWAF workspace mutation enforces analyst-session scope.
- **DuckDB user-SQL safety**:
  - New `backend/utils/sql_validator.py` enforces a statement-type whitelist + recursive parse-tree walker with catalog blocklist (`duckdb_*` / `pg_*` prefixes, `information_schema` / `pg_catalog` / `system` schemas, non-`main` catalogs) + function denylist (`read_csv` / `read_parquet` / `iceberg_scan` / `glob` / `lsdir` / `getenv` / `current_setting` / `duckdb_secrets` / postgres / sqlite / mysql scanners) + fail-closed parse + audit logging + perf budget. Replaces a regex-based blocklist that missed `read_csv_auto`, `information_schema`, `duckdb_secrets`, `INSTALL/LOAD`, and `getenv`.
  - `escape_sql_literal` helper applied at four ingest call sites; characterisation tests cover the PoC payload + multi-byte UTF-8 + backslash + empty + long-with-many-quotes.
  - `time_range` validated via `dateutil.isoparse` before SQL interpolation.
  - `get_con` / `get_meta_con` dropped the auto-query-param `read_only` flag.
- **VCL header & cache discipline**:
  - `vcl_recv` preamble unsets every internal `x-of-*` / `x-fos-edge-data` / `x-is-cluster-fetch` / `X-Edge-*` header on the inbound request.
  - Origin-metric VCL fields: numeric regex gates + `json.escape` on string values (log-injection).
  - VCL ua/referer keeps its `substr` cap.
  - Fastly `vcl_hash` now keys on the full `req.url` (path + query), not just `req.url.path` — closes cross-query cache poisoning. Auth `key` querystring is already stripped earlier so no secrets leak into cache keys.
- **Cross-tenant scope enforcement**:
  - `/api/alerts/*` and `/api/views/*` enforce analyst-session scope on every read and mutation; pre-flight scope check on PATCH / DELETE via new `get_alert_by_id` / `get_view_by_id` helpers so unauthorised mutations never land.
  - `/api/sources`, `/api/log-fields/catalog`, NGWAF workspace listing — analyst-scope filtering.
  - Cache-layer audit confirmed every per-tenant cache (`session_scoring._cached`, iceberg, bot_sources) includes `service_id` in the key.
- **Path-traversal cages**:
  - `/api/download` path traversal: `realpath` + `commonpath` cage.
  - Cache cleanup rejects bucket separators + `realpath` cage.
  - `service_id` alphanumeric/dash/underscore validation in path helpers.
- **Secret & data hygiene**:
  - `claim_token` TOCTOU → atomic UPDATE with rowcount check.
  - `share_db` quarantine narrowed to actual SQLite corruption signatures (was wiping the DB on transient `OperationalError`).
  - Email-enumeration timing equalised via dummy scrypt on miss.
  - `validate_session` re-syncs `pii_policy` / window / `service_ids` on every call so admin permission edits take effect immediately.
  - `_StaticAssetLimiter` bounded at 10 k tracked IPs.
  - `logging-settings/update` moved GET → POST/PATCH (CSRF).
  - `query_errors` decorator logs traceback server-side, never in the response body; sweep fixture asserts no `trace` key leaks from any route.
- **SSH host-key pinning**: `configs/ssh_known_hosts` pinned, source-controlled, and gitignore-excepted; tunnel manager refuses to start when the file is missing (fail-safe; no TOFU fallback).
- **Scorer signal tightening**: Python + Rust parity — `L1_SCORE_COOKIE_TAMPERED = 100` (was capped at 75 with missing/expired); `L1_ROBOTIC_DWELL_LOW_S 0.5 → 0.20` (closes the 0.20s–0.50s robotic-bot threshold gap). Tracked follow-up sliding-window mean (needs cookie-schema v3) — partial mitigations via `SESSION_IDLE_EXPIRE_S=30 min` + `SESSION_HARD_CAP_S=24h` + session-max scoring bound the practical attack window.

### Tests

- 3070 backend tests
- 65 scorer Rust tests (+8)
- 265 frontend vitest tests (+13)
- `make ci` green: lint + format + mypy + pytest + vcl-test + verify-deps + typecheck-frontend + test-frontend + osv.

### Infrastructure

- Backend Docker image: `python:3.12-slim-bullseye` → `python:3.12-slim-bookworm` (cuts CVE-laden Debian 11 base; remaining 13 high CVEs are deep-dependency / OpenSSL CVEs every major Python base inherits). Frontend image's api-schema stage bumped to match.
- Backend image now ships [`falco`](https://github.com/ysugimoto/falco) v2.3.0 (Fastly VCL static analyser) — required by the scoring-recv-snippet validator.
- **Secret scanning** — [`gitleaks`](https://github.com/gitleaks/gitleaks) v8.30.1 wired in three places: `.pre-commit-config.yaml` (blocks accidentally-staged credentials at commit time), `make secret-scan` Makefile target chained into `make ci`, and a dedicated step in `.github/workflows/ci.yml` (fails the build on any non-allowlisted finding). Configuration in `.gitleaks.toml` extends the built-in ruleset and adds path allowlists for tracked test fixtures, Rust lockfile checksums, the public SSH host key, and (for working-tree-only scans) the gitignored real-config / `.next/` / `data/system/` directories. Verified clean against the full branch history. Policy + suppression playbook documented in **AGENTS.md** §Secrets.
- **CDN cache-key hardening** — `backend/core/fastly/utils.py` `vcl_recv` now runs `querystring.filter_except` to drop all non-S3-API query parameters (caller-injected tracking params, marketing UTMs, session IDs) BEFORE the cache lookup, followed by `querystring.sort` to canonicalise the remaining param order. Composes with the `vcl_hash` fix: untrusted params can no longer fracture the cache OR leak the auth `key` into the cache key.
- Dependency freshness sweep on all four ecosystems:
  - **Python:** `aiohttp 3.13.5 → 3.14.0`, `cfn-lint 1.51.2 → 1.51.4`, `distlib 0.4.0 → 0.4.1`, `filelock 3.29.0 → 3.29.1`, `idna 3.17 → 3.18`, `joserfc 1.6.8 → 1.7.0`.
  - **Frontend:** `@tanstack/react-query 5.100.14 → 5.101.0` (+ devtools), `@types/react 19.2.15 → 19.2.16`, `react/react-dom` resolved to `19.2.7` via the existing `^19.2.5` range. `next` + `eslint-config-next` stay pinned at `16.2.6`.
  - **Rust:** `bitflags 2.11.1 → 2.12.1`.
  - **Deferred (major bumps reserved for 1.2):** TypeScript 5.9 → 6.0 (compiler-API breaking changes); Fastly Rust SDK 0.11 → 0.12 (Compute@Edge API changes); jsdom / eslint / vitest where we're already ahead of the npm "latest" tag.

### Known limitations

- Rate limiting at the edge is NOT included. The DDoS gate (`fastly.ddos_detected`) handles attack-scale traffic by bypassing Compute; sustained-low-rate abuse is left to the operator's existing WAF/NGWAF policies. A future rate-limiting feature is tracked separately.
- When a matrix is rolled back via the UI, the edge Wasm continues to use its embedded matrix until `scripts/scoring/deploy_wasm.sh` re-runs. The Restore endpoint returns a `deploy_hint` with the exact command. See `docs/session_scoring_runbook.md`.

[1.1.0]: https://github.com/fastly/fastly-log-analytics/releases/tag/v1.1.0

## [1.0.0] - 2026-06-01

Initial public release. Self-hosted dashboard for searching, filtering, and visualizing request-level Fastly logs streamed to Fastly Object Storage.

### Highlights

- **Apache Iceberg data lake** in Fastly Object Storage — ACID-compliant log storage, safe for concurrent readers and writers, with automated compaction and snapshot expiration.
- **Automated provisioning** — guided wizard (and equivalent `backend/provision.py` CLI) creates the FOS bucket, scoped access key, CDN-fronting Fastly Delivery service, and the logging endpoint on your VCL service. Auto-rollback on failure.
- **Crash-safe ingestion** — buffered locally, atomically committed; interrupted imports never corrupt the table.
- **CDN-accelerated reads** — every FOS data read goes through a Fastly Delivery service for free egress and edge caching.
- **Multi-source support** — analyze logs from multiple Fastly services side by side, each with its own DuckDB engine and Iceberg table.
- **Interactive dashboards** — traffic over time, global request map, top-N aggregations across every dimension, paginated raw-log viewer with click-to-filter.
- **Insights** — automated anomaly detection for error spikes, regional traffic surges, new IPs, WAF signal changes, cache efficiency collapses, and latency regressions.
- **Usage & Cost** — live storage breakdown, FOS Class A / B operation counts, period totals, and an interactive cost estimator pre-filled from your traffic stats.
- **Log-line accounting** — reconciles Fastly's authoritative `/stats/service/{id}` counter against locally-ingested rows bucket-by-bucket and surfaces sustained pipeline loss.
- **Configurable log fields** — thirteen built-in field groups (HTTP, network, geo, TLS, NGWAF, QUIC/HTTP3, origin metrics, etc.) plus arbitrary custom VCL fields with auto-generated Edge Data Capture snippets.
- **Alerts** — threshold-based, webhook-delivered, with optional comparison-period evaluation and per-status-code scope.
- **Two collaboration modes** — invite analysts to run an independent copy (durable JSON-config join with read-only FOS credentials), or share your running instance live via three sharing modes: SSH reverse tunnel via localhost.run, your own hostname, or your own public IP. Per-analyst passcode invites, optional IP allowlist, optional expiry, and instant single-invite or sever-all revoke. Per-mode trust-model trade-offs are documented in [SECURITY.md](SECURITY.md#live-dashboard-sharing--trust-model).
- **Field-size guard** — warns when your selected log fields approach Fastly's ~8 KB log-format limit.

See [docs/features.md](docs/features.md) for the full feature reference.

[1.0.0]: https://github.com/fastly/fastly-log-analytics/releases/tag/1.0.0
