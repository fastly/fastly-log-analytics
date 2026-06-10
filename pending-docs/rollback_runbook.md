# Rollback Runbook — v2.0 Cleanup

Each cleanup phase that touches storage or schema produces a snapshot before deploy and a phase-specific entry below. The entries are imperative — copy/paste runnable.

## Conventions

- **Prod snapshot root on the VM:** `/mnt/app-data/snapshots/`
- **Local backup snapshot root (this dev box):** `~/snapshots/`
- Snapshot naming: `<phase-tag>-<UTC-timestamp>/`
- Each snapshot contains: per-service DuckDB files, Iceberg catalog SQLite, `backend.db`, any phase-specific extras
- Restart sequence after restoring: `~/restart.sh` (per `gce-deploy-rebuild` memory) — fetches, rebuilds, healthchecks
- Browser: hard-refresh after any restart (per `gce-deploy-rebuild` memory)

## Pre-v2.0 cutover: prod snapshot → dev sync workflow

Before testing the v2.0 cleanup branch against real data, take a backup snapshot of prod state to this dev box AND sync the same data into dev for testing. The backup snapshot is the rollback target if either the dev test OR the eventual prod deploy goes wrong.

```bash
# One command does both: tar prod's /mnt/app-data → ~/snapshots/pre-v2.0-cutover-<ts>/
# then re-streams the same data into local data/cache/configs (with the
# dev-sandbox scrub applied to configs). Refuses to run if a local backend
# is using data/.
scripts/dev/snapshot_prod_to_dev.sh

# If the dev tree gets into a bad state mid-testing, roll back:
./run.sh --kill
scripts/dev/restore_dev_from_snapshot.sh ~/snapshots/pre-v2.0-cutover-<ts>
./run.sh

# The snapshot is kept on disk after restore so you can restore again
# (e.g. to retry an upgrade-path test from a clean baseline).
```

The local snapshot is also the rollback target if the eventual GCE deploy goes wrong **AND** prod's own `/mnt/app-data/snapshots/` was somehow lost. Treat it as the off-VM backup of record for the cutover window.

## Generic rollback (any phase)

If a deploy is bad and the snapshot was taken correctly:

```bash
# 1. Stop the stack
ssh vm
cd /path/to/repo
docker compose down

# 2. Reset code to the pre-deploy commit
git fetch
git reset --hard <commit-sha-before-deploy>

# 3. Restore data snapshot (only required if the bad deploy mutated on-disk state)
SNAP=/mnt/app-data/snapshots/<phase-tag>-<timestamp>
sudo tar -xzf $SNAP/data.tar.gz -C /mnt/app-data/

# 4. Bring the stack back up
./restart.sh

# 5. Smoke
curl -fsS localhost/healthz
curl -fsS localhost/api/services | jq '.[].id'
```

If the bad deploy did NOT mutate on-disk state (frontend-only change, route added, etc.), skip step 3.

## Phase 0 — pre-merge snapshot (`performance-improvement` → `main`)

**Status:** completed prior to Phase 0 start.

```bash
TS=$(date -u +%Y%m%dT%H%M%SZ)
SNAP=/mnt/app-data/snapshots/pre-cleanup-$TS
sudo mkdir -p $SNAP
sudo tar -czf $SNAP/data.tar.gz \
    --exclude='*/sessions/*' \
    /mnt/app-data/services \
    /mnt/app-data/iceberg_catalog.db \
    /mnt/app-data/backend.db
echo "$SNAP"  # record the path
```

**Rollback:** `git reset --hard <pre-merge-sha>; tar -xzf ...; ./restart.sh`. Tested locally on a copied catalog before the merge ran.

## Phase 4 — pre-storage-carve-up

Take snapshot BEFORE `git pull` to the Phase 4 commit. The storage carve-up may change catalog access patterns; if 4.1 introduces catalog DDL, the migration runs on first boot via `sqlite_migrations.py`.

```bash
TS=$(date -u +%Y%m%dT%H%M%SZ)
SNAP=/mnt/app-data/snapshots/phase-4-$TS
sudo mkdir -p $SNAP
sudo tar -czf $SNAP/data.tar.gz \
    /mnt/app-data/services \
    /mnt/app-data/iceberg_catalog.db \
    /mnt/app-data/backend.db
```

**Rollback indicators:** F3 wedge returns (read p95 > 5s sustained), Iceberg view-rebuild errors in logs, pool thread-wait p95 > 200ms.

**Recovery:** generic rollback above. The Phase 4 catalog migration is forward-only; restoring the snapshot replaces the migrated catalog with the pre-migration one cleanly.

## Phase 6 — pre-cron-isolation

Take snapshot BEFORE deploy. If 6.2 moves cron progress to SQLite, the migration is applied via `sqlite_migrations.py` on first boot.

```bash
TS=$(date -u +%Y%m%dT%H%M%SZ)
SNAP=/mnt/app-data/snapshots/phase-6-$TS
sudo mkdir -p $SNAP
sudo tar -czf $SNAP/data.tar.gz \
    /mnt/app-data/services \
    /mnt/app-data/backend.db
# cron_runs history rows are inside backend.db; covered.
```

**Rollback indicators:** cron jobs stop firing, cron-progress UI shows stale state, 503s on read endpoints during cron windows (cron isolation regression).

**Recovery:** generic rollback above. If cron progress was migrated to a new table, the snapshot's `backend.db` restores the pre-migration shape cleanly.

## Phase 8 — hard cutover

No on-disk state mutation. Rollback is code-only.

**Rollback indicators:** frontend 404s on composite endpoints, `_meta_con` removal regression (metadata routes 500), `AnalyticsDeps` alias drop breaks an internal caller.

**Recovery:**

```bash
ssh vm
cd /path/to/repo
git fetch
git reset --hard <commit-before-phase-8>
./restart.sh
```

External integrators who haven't migrated may continue to see 404s on granular endpoints. The 24-48h advance notice (CHANGELOG + README migration section + direct outreach) is the only mitigation.

## Phase 10 — final

No on-disk state mutation. Rollback is code-only.

**Recovery:** generic code-only rollback as in Phase 8.

---

## Test the rollback before relying on it

Per the planning round: each pre-deploy snapshot should be exercised on a copied catalog locally before the prod deploy. The local exercise:

1. Copy the live snapshot to a sandbox VM (or local docker volume) and untar it.
2. Run the migration that the Phase ships (if any).
3. Smoke through dashboard / security / query / admin.
4. Apply the rollback (`git reset --hard <pre>` + `tar -xzf <pre-snapshot>`).
5. Re-smoke. Confirm everything still works.

A rollback that hasn't been exercised is not a rollback; it's a promise.
