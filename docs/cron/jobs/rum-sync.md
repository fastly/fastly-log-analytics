# RUM Faro Bundle Reconciliation & Shared Parsing Contract (`backend/cron/jobs/rum_sync.py`)

> RUM discovery and ingestion run through [`rum_discovery_{service_id}`](rum-discovery.md) and the stateless ingest worker (`python -m backend.high_scale.worker`), with [`ledger_rum_sweep_{service_id}`](ledger-rum-sweep.md) as the automated crash-recovery sweep.

---

## 1. Overview & Objectives
- **Module:** `backend/cron/jobs/rum_sync.py` & `backend/core/rum_ingest.py`
- **Category:** Real User Monitoring (RUM) Faro Bundle Reconciliation & Beacon Parsing
- **Purpose:**
  1. Reconciles the self-hosted Grafana Faro Web SDK bundle in Fastly Object Storage (`_reconcile_faro_bundle`) and purges the CDN surrogate key (`_faro_purge_surrogate_key`) when the bundle changes.
  2. Provides the shared RUM beacon parser (`_parse_rum_beacon_file`, `_parse_rum_line`) and exact-byte quarantine capture (`_quarantine_rum_corrupt_lines`) used when ingesting `raw/rum/**/*.{zst,gz}` objects into ClickHouse (`rum_vitals` and `rum_errors`).

---

## 2. Shared RUM Parsing & Quarantine Contract
1. **Incremental Minute-Prefix Discovery:**
   - Discovery lists recent minute-prefixes (`rum_minute_list_prefix`, last 5 minutes) first, with a 4-hour `StartAfter` lookback bound (`Trap #44`).
2. **Dual-Domain Shared Object Handling (`Trap #48`):**
   - Because a single `raw/rum/` object can carry both Web Vitals (`rum_vitals`) and JS Errors (`rum_errors`), the claiming worker decodes both domains from the payload, publishes sibling-domain rows under `build_batch_id_from_manifest_id(f"{manifest_id}:{sibling}")`, and verifies both before acknowledging the source.
3. **Exact-Byte Quarantine:**
   - Every line that is invalid JSON, fails to parse, or is missing a valid timestamp goes into `corrupt_lines` with a categorized reason and exact-byte local capture via `_quarantine_rum_corrupt_lines`.
   - Corrupt compressed containers go through `_capture_corrupt_container(service_id, "rum", ...)`.
4. **Faro Bundle Integrity (`_reconcile_faro_bundle`):**
   - Ensures the pinned Faro SDK bundle is present in FOS and live VCL routes to it.
   - A Faro-only issue sets an otherwise successful discovery run to `"warning"`, while any data-plane failure records `"error"`.
