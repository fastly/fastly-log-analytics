# `scripts/`

Repo tooling: CI gates, build steps, operational tasks, and dev/perf harnesses.
Run from the repo root — Python via `uv run python scripts/<name>.py`, shell via
`bash scripts/<name>.sh`. Anything that talks to a live service reads its target
from env vars or config, never hard-coded infra.

## CI / build / git hooks (load-bearing — removing these breaks a gate or the build)

| Script | Wired into |
|---|---|
| `generate_openapi.py` | backend + frontend `Dockerfile` (build step), `frontend` `gen:types`, the `regen-openapi` pre-push hook |
| `check_eslint_count.sh` | `make ci`, `ci.yml` — frontend ESLint ceiling ratchet (drive-to-zero) |
| `check_no_console_otel.sh` | `make ci`, `ci.yml` — guards against `OTEL_EXPORTER=console` in deployed env (ADR-08) |
| `check_no_handrolled_retry.sh` | `make ci`, `ci.yml` — enforces shared `backend.core.retry` helper over ad-hoc retry loops |
| `check_no_inline_metric_coalesce.sh` | `make ci`, `ci.yml` — enforces canonical metric expression helpers over inline SQL `COALESCE` |
| `check_no_raw_dateparse.sh` | `make ci`, `ci.yml` — enforces `backend.utils.date_utils.parse_iso_utc` over raw date parsing |
| `check_no_raw_urllib.sh` | `make ci`, `ci.yml` — forbids raw `urllib.request` in favor of shared HTTP helpers |
| `check_security_regression_count.sh` | `make ci` + `ci.yml` — floor on security-regression test count |
| `check_test_collect_budget.sh` | `make ci`, `ci.yml` — enforces pytest collection-time and test-file-count budgets |
| `check_osv.py` | `make ci`, `ci.yml` — runs `osv-scanner`, derives severity, fails on threshold |
| `perf_gate.sh` | `perf-nightly.yml`, `make` — compares `tests/perf/latest.json` against `baseline.json` |
| `emit_perf_latest.py` | emits `tests/perf/latest.json` for the gate above (`--rows N` / `PERF_NUM_ROWS`) |
| `refresh_fastly_cidrs.py` | `cidr-refresh.yml` weekly workflow (`--check` mode also runnable locally) |
| `run_contract_backend.py` | frontend vitest `setup-backend.ts` + Playwright `global-setup.ts` + `e2e.yml` |

## Operational (run by an operator; not in CI)

| Script | Purpose |
|---|---|
| `backup_service_configs.sh` | Off-VM backup of service configs to object storage (DR; ADR-13). Writes a freshness marker the admin health card reads. |
| `analyze_web_vitals.py` | Report over the RUM Web Vitals JSONL sink (`--out`, `--purge`). |
| `admin_certificates.py` | Mints long-lived CA, server, and client mTLS certificates for the admin gateway (`docs/deploy/admin-mtls.md`). |
| `deploy_admin_gateway.py` | Provisions and activates the mTLS admin gateway in Standard or High-Scale deployments. |
| `validate_admin_gateway.sh` | End-to-end validation of the deployed mTLS admin gateway listener and certificate chain. |
| `setup_pg_schema.py` | Idempotent PostgreSQL 16 metadata schema bootstrap (`metadata-schema-init` container / manual init). |
| `backfill_high_scale.py` | Backfills High-Scale ClickHouse tables from archived or FOS raw log batches. |
| `clickhouse_replay.py` | Replays quarantined or archived batches into High-Scale ClickHouse tables. |
| `recover_high_scale_rum_vitals.py` | Re-inspects and recovers High-Scale RUM vitals/errors cursor windows into ClickHouse. |
| `verify_remote_high_scale.py` | Verifies remote High-Scale ClickHouse serving parity, health, and ingestion state. |
| `verify_dashboard.js` | Playwright dashboard & role verification across local and remote environments. |

## Dev & perf harnesses (`dev/` + load tooling)

| Script | Purpose |
|---|---|
| `dev/sync-from-remote.sh` | Pull prod data/cache/configs into local dev (scrubs configs). Used by `snapshot_prod_to_dev.sh` below. |
| `dev/snapshot_prod_to_dev.sh` | Snapshot prod, then sync into dev (rollback runbook). |
| `dev/audit_environments.py` | Read-only environment parity and configuration audit. |
| `dev/measure_e2e_freshness.py` | End-to-end ingestion freshness probe across environments. |
| `dev/verify_observability.py` | Diagnostic CLI verifying OTel traces, Prometheus metrics, and structured log delivery. |
| `dev/scale_harness.py` | Bounded staged edge-load runner with FOS freshness, cron, pool, OTel, and analytics checkpoints. |
| `load_test/generate_synthetic_traffic.py` | Unified multi-tier synthetic traffic and log generator for DuckLake, FOS, and ClickHouse. |
| `load_test/seed_local_clickhouse.py` | Seeds local ClickHouse tables with deterministic synthetic traffic for dev and benchmarks. |

The scale harness defaults to `100:10,500:10,1000:10` stages. It measures
achieved RPS separately from the requested target and writes JSON plus
Markdown output. Stages above 1,000 RPS require `--allow-high-rate`; use
`--max-in-flight` to keep client pressure bounded:

```bash
uv run python scripts/dev/scale_harness.py run \
  --url "$LOADTEST_URL" \
  --service-id "$FASTLY_SERVICE_ID" \
  --stages 100:30,500:30,1000:30 \
  --start-time "$START_UTC" --end-time "$END_UTC" \
  --output performance-report/scale-harness.json
```

The harness waits 60 seconds after the last stage by default before taking a
final checkpoint, allowing delayed FOS delivery to be distinguished from
conversion/commit latency. Override with `--settle-seconds 0` only when a
post-run freshness checkpoint is not needed.

> Local-only artifacts (e.g. a deployed-`restart.sh` snapshot) are gitignored and
> not listed here.
