# VCL Reconciliation Migration Guide (v2.3)

## Overview

Version 2.3 introduces a **declarative VCL reconciliation system** that automatically detects and removes legacy VCL snippets from pre-2.3 deployments, replacing them with consolidated, feature-unified snippets. This guide explains how the migration works and what users will experience.

## What Changed

### Pre-2.2 (Legacy) VCL Snippets
Services deployed before v2.3 have multiple separate VCL snippets:
- `RUM - recv`, `RUM - miss`, `RUM - fetch`, `RUM - deliver`, `RUM - error`
- `Session Scoring - recv`, `Session Scoring - miss`, etc.
- `CMCD - recv`, `CMCD - miss`, etc.
- `Fastly Log Analysis - recv`, `Fastly Log Analysis - miss`, etc.

**Total:** ~20 separate snippets managing overlapping concerns.

### v2.3+ (Consolidated) VCL Snippets
All features are consolidated into 5 unified snippets:
- `Fastly Log Analytics - vcl_recv`
- `Fastly Log Analytics - vcl_miss`
- `Fastly Log Analytics - vcl_fetch`
- `Fastly Log Analytics - vcl_deliver`
- `Fastly Log Analytics - vcl_error`

**Benefits:**
- Eliminates duplication and potential conflicts
- Single source of truth for VCL ordering (RUM before Session Scoring before standard logging)
- Easier to reason about and debug
- Cleaner Fastly service configuration

## Automatic Migration Path

When a user upgrades to v2.3 and deploys via the admin UI:

### Step 1: Detection
The reconciliation system reads the current Fastly service state and detects legacy snippets.

### Step 2: Auto-Cleanup
If legacy snippets exist AND no consolidated snippets are present, they are automatically queued for removal.

### Step 3: Deployment
The reconciliation proceeds with the 8-step control loop:
1. Acquire lock
2. Build desired state
3. Fetch current state
4. **Compute diff + detect legacy snippets** ← NEW
5. Clone active version to draft
6. Apply diff (removes legacy, adds consolidated)
7. Validate draft VCL
8. Activate draft

### Step 4: Verification (UI Banner)
After deployment, the admin sees:
- ✓ **Clean state** — if no legacy snippets remain
- ⚠ **Migration in progress** — if some legacy remain (re-run reconciliation)
- ⚠ **Pre-migration** — if only legacy exist (run reconciliation to start)

## How Users Upgrade

### Via Admin UI (Recommended)

1. **Download v2.3 codebase**
   ```bash
   git pull origin main
   # Backend auto-deploys to GCE
   ```

2. **Navigate to Provisioning Page**
   - Admin → Provision → Select existing service

3. **Trigger Reconciliation**
   - Click "Reconcile VCL" or "Re-Deploy"
   - The UI streams reconciliation progress in real-time

4. **Verify Clean State**
   - Admin → System Health → VCL Health Check
   - Shows: "✓ Clean state — VCL migration complete"

### Via API (CLI or Automation)

```bash
# Check current VCL state
curl -X GET "http://localhost:8000/api/admin/vcl-health?service_id=svc_123"

# Response example:
{
  "service_id": "svc_123",
  "active_version": 15,
  "legacy_snippets_found": 3,
  "consolidated_snippets_found": 0,
  "is_clean": false,
  "recommendation": "⚠ Pre-migration state: 3 legacy snippet(s) found. Run reconciliation to migrate to consolidated VCL."
}

# Trigger reconciliation
curl -X POST "http://localhost:8000/api/provision/reconcile?service_id=svc_123&token=YOUR_FASTLY_TOKEN"
# Streams SSE events with progress
```

## Technical Details

### Legacy Snippet Detection

The reconciler identifies legacy snippets by name prefix:
- `"Fastly Log Analysis"` (v1 logging)
- `"RUM -"` (Real User Monitoring)
- `"Session Scoring"` (Session risk scoring)
- `"CMCD"` (Common Media Client Data)

### Smart Cleanup Logic

```python
if legacy_snippets_found AND not consolidated_snippets_found:
    # First deployment with v2.3: auto-remove legacy
    queue_legacy_for_removal()
else if legacy_snippets_found AND consolidated_snippets_found:
    # Partial migration detected: skip auto-removal
    # User must manually complete reconciliation
    log_warning("Legacy and consolidated both found")
```

This prevents accidents during rollback scenarios where both sets might momentarily coexist.

### Endpoints

**New Endpoints:**

- `POST /api/provision/reconcile` — Trigger VCL reconciliation (SSE stream)
  - Parameters: `service_id`, `token`
  - Response: SSE events (`status`, `reconciliation_result`, `error`, `done`)

- `GET /api/admin/vcl-health` — Check VCL migration status
  - Parameters: `service_id`
  - Response: JSON with `legacy_snippets_found`, `consolidated_snippets_found`, `is_clean`, `recommendation`

## Migration Safety Guarantees

1. **Idempotency**: Running reconciliation twice is safe; second run is a no-op.
2. **Validation**: Draft VCL is validated before activation. Broken VCL never goes live.
3. **Atomicity**: Legacy removal and consolidated addition happen in single version bump.
4. **Rollback**: If reconciliation fails, active version stays unchanged. Manual rollback not needed.
5. **Customer Origins Protected**: Only Fastly Log Analytics–managed backends/dictionaries can be deleted. Customer origins are never touched.

## Testing the Migration

### Dev Environment

```bash
# Simulate legacy deployment
# 1. Create service with old config format
# 2. Deploy to Fastly manually with legacy snippets
# 3. Update local config to v2.3
# 4. Run reconciliation dry-run

uv run pytest tests/backend/provision/declarative/test_reconciler_integration.py::TestMigrationFromLegacyToConsolidated -v
```

### Staging/Production

1. **Dry-run reconciliation first:**
   ```bash
   POST /api/provision/reconcile?service_id=svc_xyz&token=TOKEN&dry_run=true
   ```
   Shows what will change without applying it.

2. **Verify VCL diff** in SSE stream output.

3. **Run live reconciliation:**
   ```bash
   POST /api/provision/reconcile?service_id=svc_xyz&token=TOKEN
   ```

4. **Monitor for 5 minutes:**
   - Check for 5xx errors in logs
   - Verify ingest jobs still fire
   - Confirm no spike in Fastly API errors

5. **Health check:**
   ```bash
   GET /api/admin/vcl-health?service_id=svc_xyz
   ```
   Should return `is_clean: true`.

## FAQ

### Q: Will my service experience downtime during migration?
**A:** No. Reconciliation clones the active version to a draft, makes changes, validates, then atomically activates. The swap is instantaneous on Fastly's edge.

### Q: What if reconciliation fails?
**A:** The active version remains unchanged. No rollback needed. Fix the issue and re-run reconciliation.

### Q: Can I rollback to v2.1?
**A:** Yes, but you'll need to manually restore your old VCL snippets from git history or revert the Fastly service version. The reconciliation system only works forward (legacy → consolidated).

### Q: Will my custom VCL be affected?
**A:** No. Only Fastly Log Analytics–managed snippets (those in the whitelist) can be deleted. Your custom VCL is untouched.

### Q: Do I need to do anything manually?
**A:** For most users: no. Just update your codebase and re-deploy. The reconciliation runs automatically on next provision.

For CLI users: run `python -m backend.provision.cli reconcile --service-id <id> --token <token>` or use the `/api/provision/reconcile` endpoint.

### Q: What about services with mixed legacy/consolidated snippets?
**A:** This indicates a partial migration. The reconciler will not auto-remove legacy in this case (safety check). You must manually complete the reconciliation or contact support.

## Release Notes

**v2.3.0 - VCL Reconciliation System**

- ✅ Consolidated 20 separate VCL snippets into 5 unified ones
- ✅ Automatic migration from pre-2.2 legacy snippets
- ✅ Safe, idempotent reconciliation control loop
- ✅ VCL health check endpoint for post-deploy verification
- ✅ Streaming reconciliation API for UI feedback
- ✅ Full test coverage of migration scenarios

**No breaking changes.** Existing deployments migrate transparently on next reconciliation.
