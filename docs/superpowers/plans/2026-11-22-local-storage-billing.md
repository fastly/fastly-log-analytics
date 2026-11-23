# Local Storage and Operations Billing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Platform-account storage overrides with service-local, selected-period byte-hour totals that include live and deleted-but-still-billed objects in both High-Scale and Cloud.

**Architecture:** Both editions produce the same `StorageBillingInterval` wire shape from edition-owned lifecycle data. Cloud reads a background-reconciled owned-object inventory plus ingest/FOS deletion logs; High-Scale reads Postgres source/archive ledgers. A shared frontend calculator integrates intervals across the selected range, and every Usage cost surface consumes that one local total. Selected-period incurred operations also use the service-local usage log in both editions.

**Tech Stack:** Python 3.12, FastAPI/Pydantic, SQLite/Postgres, Rust/Fastly Compute, TypeScript, React Query, Vitest, pytest.

## Global Constraints

- Do not use Fastly Platform `object_storage_bytes_count` or `storage.gb_hours` as an authority for Storage Impact, Estimated Incurred Cost, or either Cost Estimator.
- The Usage page reads the latest complete local inventory snapshot; do not perform a full on-demand bucket LIST in the page request path.
- Target cold Usage-page completion of 20 seconds or less from navigation once the local inventory snapshot is current.
- Keep account-wide Platform storage visible only as a separately labeled informational card.
- Integrate billable byte-time over the selected `[start, end)` interval.
- Include every live owned object and every locally recorded deletion still inside its minimum billing interval.
- Mark incomplete lifecycle coverage explicitly; never render an exact zero or fabricated exact interval.
- Keep Cloud and High-Scale implementations on the same response contract and frontend calculator.
- Do not delete app data.
- Use TDD for every behavior change.
- Run commands serially and bounded; do not start a fleet or parallel test/build workers.

---

### Task 1: Add the shared storage interval calculator

**Files:**
- Create: `frontend/lib/cloud-engine/usage/storageBilling.ts`
- Create: `frontend/lib/cloud-engine/usage/storageBilling.test.ts`

**Interfaces:**
- Consumes: `StorageBillingInterval[]` and selected period epoch milliseconds.
- Produces: `StorageBillingSummary` with `liveGbHours`, `deletedGbHours`, `minimumBilledGbHours`, `totalGbHours`, `complete`, and `provenance`.

- [ ] **Step 1: Write failing overlap and completeness tests**

```ts
import { calculateStorageBilling } from "./storageBilling";

it("integrates only selected-period overlap from local lifecycle intervals", () => {
  const GiB = 1024 ** 3;
  const hour = 3_600_000;
  const summary = calculateStorageBilling(
    [
      {
        id: "live-a",
        dataset: "pq_request",
        kind: "live",
        bytes: GiB,
        billedFrom: 10 * hour,
        billedUntil: 20 * hour,
        complete: true,
        provenance: "cloud:inventory",
      },
      {
        id: "deleted-a",
        dataset: "raw_request",
        kind: "minimum_billed",
        bytes: 2 * GiB,
        billedFrom: 11 * hour,
        billedUntil: 13 * hour,
        complete: true,
        provenance: "cloud:ingest_files",
      },
    ],
    12 * hour,
    14 * hour,
  );

  expect(summary).toEqual({
    liveGbHours: 2,
    deletedGbHours: 0,
    minimumBilledGbHours: 2,
    totalGbHours: 4,
    complete: true,
    provenance: ["cloud:ingest_files", "cloud:inventory"],
  });
});

it("does not call incomplete intervals exact", () => {
  const summary = calculateStorageBilling(
    [{
      id: "deleted-incomplete",
      dataset: "pq_request",
      kind: "deleted",
      bytes: 1024 ** 3,
      billedFrom: 0,
      billedUntil: 3_600_000,
      complete: false,
      provenance: "cloud:fos_usage",
    }],
    0,
    3_600_000,
  );
  expect(summary.complete).toBe(false);
  expect(summary.deletedGbHours).toBe(1);
});
```

- [ ] **Step 2: Run the new tests and verify RED**

Run:

```bash
cd frontend
perl -e 'alarm 120; exec @ARGV' npm test -- --maxWorkers=1 lib/cloud-engine/usage/storageBilling.test.ts
```

Expected: FAIL because `storageBilling.ts` does not exist.

- [ ] **Step 3: Implement exact interval intersection**

```ts
export type StorageBillingInterval = {
  id: string;
  dataset: string;
  kind: "live" | "deleted" | "minimum_billed";
  bytes: number;
  billedFrom: number;
  billedUntil: number;
  complete: boolean;
  provenance: string;
};

export function calculateStorageBilling(
  intervals: readonly StorageBillingInterval[],
  from: number,
  to: number,
): StorageBillingSummary {
  // Clamp invalid/negative bytes, reject invalid ranges, intersect each
  // interval with [from, to), aggregate by kind in GiB-hours, and mark the
  // result complete only when every contributing interval is complete.
}
```

The implementation must reject `to <= from`, ignore empty intersections, deduplicate interval IDs, sort/deduplicate provenance, and avoid rounding until presentation.

- [ ] **Step 4: Run focused tests and verify GREEN**

Run the Task 1 test command. Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add frontend/lib/cloud-engine/usage/storageBilling.ts frontend/lib/cloud-engine/usage/storageBilling.test.ts
git commit -m "Add selected-period storage billing calculator"
```

### Task 2: Persist Cloud's complete live object inventory

**Files:**
- Modify: `compute/cloud/src/admin/logs.rs`
- Modify: `compute/cloud/src/ingest/retention.rs`
- Modify: `compute/cloud/src/telemetry/schema.rs`
- Modify: `compute/cloud/tests/ingest/sweep_and_api.rs`
- Modify: `frontend/lib/cloud-engine/usage/summary.ts`
- Modify: `frontend/lib/cloud-engine/usage/summary.test.ts`

**Interfaces:**
- Consumes: the six owned prefixes already defined by `retention::RECONCILE_LIST`.
- Produces: bounded `ReconcileReport.inventory_rows` batches and versioned `ops:storage_inventory` log rows containing `snapshot_id`, `dataset`, `key`, `size`, `last_modified`, and one terminal complete marker.

- [ ] **Step 1: Write the failing Rust snapshot tests**

In `sweep_and_api.rs`, extend the existing storage-reconcile tests:

```rust
#[test]
fn storage_reconcile_writes_complete_per_object_inventory_snapshot() {
    let fake = Fake::new(BASE);
    put_processed(&fake, "processed/request/", BASE, "1-2-a-n1");
    fake.bucket.borrow_mut().insert(
        "raw/request/year=2027/month=01/day=15/hour=08/minute=00/a.zst".into(),
        vec![0; 5],
    );
    fake.new_request();
    assert!(ingest::retention::request_storage_reconcile(&fake, "admin").unwrap());
    let mut rows = Vec::new();
    loop {
        fake.new_request();
        let report = ingest::retention::storage_reconcile_step(&store(&fake, &service()), 0)
            .unwrap()
            .unwrap();
        rows.extend(report.inventory_rows);
        if report.snapshot.is_some() {
            break;
        }
    }
    assert!(rows.iter().any(|row| row["dataset"] == "pq_request"));
    assert!(rows.iter().any(|row| row["key"].as_str().unwrap().starts_with("raw/request/")));
    assert_eq!(rows.iter().filter(|row| row["complete"] == true).count(), 1);
}

#[test]
fn storage_reconcile_publishes_inventory_manifest_once_after_all_prefixes() {
    let fake = Fake::new(BASE);
    put_processed(&fake, "processed/request/", BASE, "1-2-a-n1");
    fake.new_request();
    assert!(ingest::retention::request_storage_reconcile(&fake, "admin").unwrap());
    let mut terminal = Vec::new();
    for _ in 0..8 {
        fake.new_request();
        let report = ingest::retention::storage_reconcile_step(&store(&fake, &service()), 0)
            .unwrap()
            .unwrap();
        terminal.extend(report.inventory_rows.into_iter().filter(|row| row["complete"] == true));
        if report.snapshot.is_some() { break; }
    }
    assert_eq!(terminal.len(), 1);
}
```

Also add a >1,000-object case using the existing `Fake::bucket` test setup. Extend `report.inventory_rows` across passes and assert raw and processed prefixes each appear once with a stable `dataset`, byte size, RFC 3339 `last_modified`, and `snapshot_id`.

- [ ] **Step 2: Run the Rust test and verify RED**

```bash
cd compute/cloud
perl -e 'alarm 300; exec @ARGV' cargo test --test ingest storage_reconcile -- --test-threads=1
```

Expected: FAIL because reconcile still emits aggregate dataset totals only.

- [ ] **Step 3: Persist the inventory during background reconcile**

Extend `storage_reconcile_step()` in `retention.rs` so it:

1. assigns a stable `snapshot_id` at reconcile start;
2. walks the owned prefixes in fixed order across bounded cron passes;
3. returns each listed object as an inventory row in `ReconcileReport.inventory_rows`;
4. returns one terminal `complete=true` row only after every prefix reaches its terminal cursor;
5. never lists unrelated bucket prefixes.

Change `reconcile_stage()` in `admin/logs.rs` to accept its existing `LogBuffer` and call `write_log(runtime, buffer, WritableLogKind::StorageInventory, row)` for every `inventory_rows` entry. Add the row schema to `telemetry/schema.rs`. Continue returning aggregate dataset totals in the existing `storage_reconcile` `cron_runs` details so older consumers remain compatible. The terminal row is the visibility fence: readers ignore incomplete snapshot IDs.

- [ ] **Step 4: Write the failing frontend snapshot test**

In `summary.test.ts`, fake two `ops:storage_inventory` reads and prove `fetchCloudStorageInventory()` selects the newest complete `snapshot_id`, returns exact live intervals, and marks the result incomplete when the newest run lacks a terminal marker.

- [ ] **Step 5: Implement the Cloud inventory reader**

In `summary.ts`, add:

```ts
export async function fetchCloudStorageInventory(
  signal?: AbortSignal,
): Promise<StorageBillingInterval[]>
```

It must:

- read the latest completed inventory snapshot through the existing paginated telemetry reader;
- convert `last_modified` to epoch milliseconds;
- set `kind: "live"`, `billedFrom` to the object timestamp, and `billedUntil` to `now`;
- use a stable interval ID from `dataset:key`;
- scope cache/provenance to the active service identity and reject stale ops epochs.
- perform no on-demand FOS LIST; return the latest complete snapshot plus post-snapshot lifecycle deltas.

- [ ] **Step 6: Run focused Rust and Vitest tests**

Run the Task 2 Rust command, then:

```bash
cd frontend
perl -e 'alarm 240; exec @ARGV' npm test -- --maxWorkers=1 lib/cloud-engine/usage/summary.test.ts
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add compute/cloud/src/admin/logs.rs compute/cloud/src/ingest/retention.rs compute/cloud/src/telemetry/schema.rs compute/cloud/tests/ingest/sweep_and_api.rs frontend/lib/cloud-engine/usage/summary.ts frontend/lib/cloud-engine/usage/summary.test.ts
git commit -m "Persist Cloud storage lifecycle inventory"
```

### Task 3: Replace Cloud deletion snapshots with exact local intervals

**Files:**
- Modify: `frontend/lib/cloud-engine/usage/summary.ts`
- Modify: `frontend/lib/cloud-engine/usage/summary.test.ts`
- Modify: `frontend/lib/cloud-engine/adapter.ts`
- Modify: `frontend/lib/cloud-engine/adapter.test.ts`

**Interfaces:**
- Consumes: `ops:ingest_files` rows, `fos_usage` deletion rows, Cloud inventory intervals, and the 30-day minimum setting.
- Produces: `StorageBillingInterval[]` for deleted/minimum-billed bytes plus completeness metadata.

- [ ] **Step 1: Write failing exact deletion interval tests**

Replace the aggregate-only assertion in `summary.test.ts` with:

```ts
it("builds an exact billed interval from the first and deleted ingest_file states", async () => {
  const rows = [
    { timestamp: 10, raw_key: "a", raw_size: 100, state_version: 1, raw_deleted: false },
    { timestamp: 20, raw_key: "a", raw_size: 100, state_version: 2, raw_deleted: true, raw_deleted_at: 20 },
  ];
  const result = await fetchIngestDeletedIntervals(range, identity, reader);
  expect(result.intervals).toEqual([expect.objectContaining({
    id: "raw_request:a",
    bytes: 100,
    billedFrom: 10,
    billedUntil: 30 * 86_400_000 + 10,
    complete: true,
  })]);
});
```

Use an explicit `minimumBilledDays` argument so the test does not depend on global settings.

- [ ] **Step 2: Run the test and verify RED**

```bash
cd frontend
perl -e 'alarm 180; exec @ARGV' npm test -- --maxWorkers=1 lib/cloud-engine/usage/summary.test.ts -t 'builds an exact billed interval'
```

Expected: FAIL because the interval function does not exist.

- [ ] **Step 3: Implement Cloud deletion interval construction**

`fetchIngestDeletedIntervals()` must:

- retain the earliest timestamp per raw key as creation;
- retain the newest `(state_version, timestamp)` row as lifecycle state;
- emit a minimum-billed interval only when `raw_deleted` is true;
- set `billedUntil = max(deleted_at, created_at + minimumDays)`;
- mark missing size or creation evidence incomplete;
- deduplicate by raw key.

Keep `fetchIngestDeletedBytes()` as a compatibility wrapper until all callers move.

- [ ] **Step 4: Write and pass adapter current-storage contract tests**

In `adapter.test.ts`, assert `/api/usage/current-storage`:

- includes live inventory intervals plus exact deletion intervals;
- calls the shared calculator once;
- does not add live inventory bytes and ingest deletion bytes twice;
- preserves `storage_billing_complete: false` when FOS aggregate deletions are the only evidence.

- [ ] **Step 5: Replace the aggregate Cloud cost path**

In `adapter.ts`, replace `storageDeletion()` with `storageBillingIntervals()` and return:

```json
{
  "storage_billing_complete": summary.complete,
  "storage_billing_provenance": summary.provenance,
  "live_billed_gb_hours": summary.liveGbHours,
  "deleted_billed_gb_hours": summary.deletedGbHours + summary.minimumBilledGbHours,
  "total_billed_gb_hours": summary.totalGbHours
}
```

Do not use Platform storage values in this path.

- [ ] **Step 6: Run focused tests**

```bash
cd frontend
perl -e 'alarm 300; exec @ARGV' npm test -- --maxWorkers=1 lib/cloud-engine/usage/summary.test.ts lib/cloud-engine/adapter.test.ts
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add frontend/lib/cloud-engine/usage/summary.ts frontend/lib/cloud-engine/usage/summary.test.ts frontend/lib/cloud-engine/adapter.ts frontend/lib/cloud-engine/adapter.test.ts
git commit -m "Build Cloud storage costs from local lifecycles"
```

### Task 4: Build High-Scale local lifecycle intervals

**Files:**
- Create: `backend/core/storage_billing.py`
- Create: `tests/core/test_storage_billing.py`
- Modify: `backend/high_scale/postgres_control/control_plane.py`
- Modify: `backend/models/admin/operations.py`
- Modify: `backend/routers/usage.py`
- Modify: `tests/routers/test_admin_ops_and_storage.py`

**Interfaces:**
- Consumes: High-Scale source/archive Postgres records, local cloud-copy deletion rows, and selected period dates.
- Produces: the same interval dictionary keys used by `StorageBillingInterval`, plus current-storage aggregate fields.

- [ ] **Step 1: Write failing Python interval tests**

In `tests/core/test_storage_billing.py`:

```py
def test_selected_period_intersects_live_and_minimum_billed_objects() -> None:
    intervals = [
        {
            "id": "live-a",
            "dataset": "pq_request",
            "kind": "live",
            "bytes": GIB,
            "billed_from": h(10),
            "billed_until": h(20),
            "complete": True,
            "provenance": "high-scale:source_objects",
        },
        {
            "id": "deleted-a",
            "dataset": "raw_request",
            "kind": "minimum_billed",
            "bytes": 2 * GIB,
            "billed_from": h(11),
            "billed_until": h(13),
            "complete": True,
            "provenance": "high-scale:source_objects",
        },
    ]
    summary = calculate_storage_billing(intervals, h(12), h(14))
    assert summary.live_gb_hours == 2
    assert summary.minimum_billed_gb_hours == 2
    assert summary.total_gb_hours == 4
```

Add equality tests against the TypeScript fixture JSON exported from `storageBilling.test.ts` so the two implementations cannot drift.

- [ ] **Step 2: Run the Python test and verify RED**

```bash
perl -e 'alarm 180; exec @ARGV' uv run pytest -q -x tests/core/test_storage_billing.py
```

Expected: FAIL because the module does not exist.

- [ ] **Step 3: Implement the generic Python interval calculator and High-Scale query**

`backend/core/storage_billing.py` owns interval validation, overlap, aggregation, and `StorageBillingSummary`.

Add `PostgresControlPlane.storage_billing_intervals(service_id, start, end, minimum_days)`. It must:

- select live and deleted source records using `discovered_at`, `size_bytes`, status, and deletion transition;
- select archive objects using `created_at`, `artifact_size`, and `archive_expired_at`;
- account for the archive's three FOS objects without counting one manifest's artifact bytes more than once;
- use local cloud-copy deletion events for copied/superseded objects;
- mark a missing creation timestamp incomplete rather than inventing exact history.

- [ ] **Step 4: Write failing current-storage route tests**

Add tests proving:

- local High-Scale intervals determine `total_billed_gb_hours`;
- deleted source/artifact bytes contribute only inside the selected interval;
- `PlatformStorageUsage` is not consulted by `/api/usage/current-storage`;
- a Postgres lifecycle read failure returns an explicit incomplete/error result instead of account-wide or zero fallback.

- [ ] **Step 5: Implement response wiring**

Add to `CurrentStorageResponse`:

```py
storage_billing_complete: bool = False
storage_billing_provenance: list[str] = []
live_billed_gb_hours: float = 0
deleted_billed_gb_hours: float = 0
minimum_billed_gb_hours: float = 0
storage_intervals: list[StorageBillingInterval] = []
```

`usage_current_storage()` uses the local adapter's summary for these fields and leaves Platform metrics entirely in `/api/usage/platform`.

- [ ] **Step 6: Run focused backend tests**

```bash
perl -e 'alarm 300; exec @ARGV' uv run pytest -q -x tests/core/test_storage_billing.py tests/routers/test_admin_ops_and_storage.py -k 'storage'
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add backend/core/storage_billing.py tests/core/test_storage_billing.py backend/high_scale/postgres_control/control_plane.py backend/models/admin/operations.py backend/routers/usage.py tests/routers/test_admin_ops_and_storage.py
git commit -m "Calculate High-Scale storage from lifecycle ledgers"
```

### Task 5: Use local operation counts for incurred cost

**Files:**
- Modify: `backend/core/metadata/fos_usage.py`
- Modify: `backend/models/admin/operations.py`
- Modify: `backend/routers/usage.py`
- Modify: `tests/routers/test_admin_ops_and_storage.py`
- Modify: `frontend/lib/cloud-engine/adapter.ts`
- Modify: `frontend/lib/cloud-engine/adapter.test.ts`

**Interfaces:**
- Consumes: the service-local `usage_log` hourly rollup plus raw boundary rows and Cloud `fos_usage`.
- Produces: `/api/usage/operations` with `source`, `complete`, totals, and time buckets for both editions.

- [ ] **Step 1: Write the failing High-Scale authority test**

```py
def test_usage_operations_prefers_local_service_log_over_platform(client) -> None:
    con = metadata.get_con("svc-local-ops")
    con.execute(
        "INSERT INTO usage_log (timestamp, service_id, operation_class, operation_type, count, bytes) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        ("2026-11-22T12:15:00.000Z", "svc-local-ops", "A", "PUT", 7, 4096),
    )
    con.commit()
    app.dependency_overrides[get_source] = lambda: {
        "name": "svc-local-ops",
        "service_id": "svc-local-ops",
        "logging_service_id": "svc-local-ops",
    }
    with patch("backend.routers.usage._fastly_api") as platform:
        response = client.get(
            "/api/usage/operations?start=2026-11-22T12:00:00Z&end=2026-11-22T12:59:59Z&by=hour"
        )
    assert response.status_code == 200
    assert response.json()["source"] == "local"
    assert response.json()["total_class_a"] == 7
    platform.assert_not_called()
```

- [ ] **Step 2: Run the test and verify RED**

```bash
perl -e 'alarm 180; exec @ARGV' uv run pytest -q -x tests/routers/test_admin_ops_and_storage.py -k 'local_service_log_over_platform'
```

Expected: FAIL because the route still calls `_fastly_api` and the response has no `source`.

- [ ] **Step 3: Add a local bucket aggregate helper**

In `fos_usage.py`, add:

```py
def get_usage_operation_buckets(
    service_id: str,
    start: str,
    end: str,
    by: Literal["hour", "day"],
) -> dict[str, Any]
```

It must query `usage_log_hourly_summary` for fully contained hours and raw `usage_log` at both boundaries, return exact Class A/Class B totals, and label each bucket in UTC. Reuse the existing `_query_usage_log_aggregate_rollup` logic rather than issuing a full-window group-by.

- [ ] **Step 4: Make High-Scale and Cloud return one operation contract**

Update `UsageOperationsResponse` with:

```py
source: Literal["local", "fastly"]
complete: bool
provenance: list[str]
```

High-Scale returns `source="local"` and `complete=True` when its local usage DB exists, `complete=False` when the database/rollup is unavailable, and never silently renders account-wide zero as service zero. A Fastly Platform fallback, if retained, must set `source="fastly"` and `complete=False` so the card labels it.

Cloud's adapter returns `source="local"`, `complete=input.telemetryCoverage?.fos_usage === true`, and `provenance=["fos_usage"]` with the same totals/buckets.

- [ ] **Step 5: Add cross-edition response assertions**

In `adapter.test.ts`, feed the same Cloud FOS rows used by the High-Scale fixture and assert the normalized `UsageOperationsResponse` objects have equal `total_class_a`, `total_class_b`, `data`, `source`, and `complete`.

- [ ] **Step 6: Run focused tests**

```bash
perl -e 'alarm 240; exec @ARGV' uv run pytest -q -x tests/routers/test_admin_ops_and_storage.py -k 'usage_operations'
cd frontend
perl -e 'alarm 180; exec @ARGV' npm test -- --maxWorkers=1 lib/cloud-engine/adapter.test.ts -t 'usage operations'
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add backend/core/metadata/fos_usage.py backend/models/admin/operations.py backend/routers/usage.py tests/routers/test_admin_ops_and_storage.py frontend/lib/cloud-engine/adapter.ts frontend/lib/cloud-engine/adapter.test.ts
git commit -m "Use local operations for incurred usage cost"
```

### Task 6: Wire every Usage surface to the local total

**Files:**
- Modify: `frontend/app/usage/_sections/UsageClient.tsx`
- Modify: `frontend/app/usage/_sections/UsageStatCards.tsx`
- Modify: `frontend/components/CostCalculator/CostCalculator.tsx`
- Modify: `frontend/components/CostCalculator/CloudCostCalculator.tsx`
- Modify: `frontend/__tests__/app/usage-platform.test.tsx`
- Modify: `frontend/__tests__/app/usage-sessions-streaming.test.tsx`

**Interfaces:**
- Consumes: `CurrentStorageResponse.total_billed_gb_hours`, `storage_billing_complete`, `storage_billing_provenance`, plus `/operations.source` and `/operations.complete`.
- Produces: identical local storage cost in Storage Impact, Estimated Incurred Cost, High-Scale Cost Estimator, and Cloud Cost Estimator; incomplete operations never render as an exact `$0.00`.

- [ ] **Step 1: Change the tests to the required behavior**

Replace the `uses Platform gb-hours` assertions with:

```tsx
it("keeps local selected-period storage authoritative when Platform is present", () => {
  render(<UsageStatCards {...statProps} platformStorage={platform.storage} />);
  expect(screen.getByText("720.00 GB-hrs")).toBeInTheDocument();
  expect(screen.getAllByText("Measured · this install").length).toBeGreaterThan(0);
  expect(screen.queryByText("Measured · Fastly Platform (account-wide)")).not.toBeInTheDocument();
});
```

Add the same assertion to `CostCalculator` and `CloudCostCalculator`.

Add a selected-period operations test:

```tsx
it("does not present incomplete local operations as an exact zero", () => {
  render(<UsageStatCards {...statProps} totalClassA={0} totalClassB={0}
    operationsSource="local" operationsComplete={false} />);
  expect(screen.getAllByText("Unavailable").length).toBeGreaterThanOrEqual(2);
  expect(screen.queryByText("$0.00")).not.toBeInTheDocument();
});
```

- [ ] **Step 2: Run the tests and verify RED**

```bash
cd frontend
perl -e 'alarm 240; exec @ARGV' npm test -- --maxWorkers=1 __tests__/app/usage-platform.test.tsx __tests__/app/usage-sessions-streaming.test.tsx
```

Expected: FAIL because `platformStorage` still overrides local usage.

- [ ] **Step 3: Remove Platform override and thread the local value**

In `UsageClient.tsx`:

```tsx
const observedStorageGbHours = storage?.total_billed_gb_hours ?? null
```

Do not read `platform?.storage.gb_hours` for cost surfaces. Keep passing `platform` only to `PlatformUsageCards`.

In `UsageStatCards.tsx`, `CostCalculator.tsx`, and `CloudCostCalculator.tsx`, rename the prop to `localStorageGbHours`, label it `Measured · this install`, and use it as the one storage authority. Add partial storage status when `storage_billing_complete` is false.

Pass `ops.source` and `ops.complete` into `UsageStatCards`. `roughCostA` and `roughCostB` remain actual local totals only when the response is complete; incomplete operation telemetry renders `Unavailable` and is listed in `excludedCosts`. The monthly Cost Estimator remains explicitly modeled monthly usage, so it can legitimately differ from the selected-period incurred result.

- [ ] **Step 4: Run focused UI tests**

Run the Task 6 test command. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add frontend/app/usage/_sections/UsageClient.tsx frontend/app/usage/_sections/UsageStatCards.tsx frontend/components/CostCalculator/CostCalculator.tsx frontend/components/CostCalculator/CloudCostCalculator.tsx frontend/__tests__/app/usage-platform.test.tsx frontend/__tests__/app/usage-sessions-streaming.test.tsx
git commit -m "Use local storage totals for Usage costs"
```

### Task 7: Regenerate contracts, document behavior, and verify the slice

**Files:**
- Modify: generated OpenAPI and TypeScript contract files through repository scripts
- Modify: `docs/pages/usage.md`
- Modify: `notes/WIP.md` in the deploy-testing repository

**Interfaces:**
- Consumes: completed Tasks 1-6.
- Produces: generated contracts, user-facing behavior docs, and a recorded live-verification gate.

- [ ] **Step 1: Regenerate the OpenAPI artifacts**

```bash
cd frontend
perl -e 'alarm 240; exec @ARGV' npm run gen:types
perl -e 'alarm 180; exec @ARGV' npm run gen:cloud-types
```

Expected: both commands pass and only contract files change.

- [ ] **Step 2: Add the user-facing Usage contract**

Update `docs/pages/usage.md` to state:

- Storage cost is service-local and selected-period;
- live and deleted-but-still-billed bytes are included;
- Fastly Platform storage is account-wide informational data;
- incomplete lifecycle coverage is shown as partial, never exact zero;
- Cloud and High-Scale share the same response semantics.

- [ ] **Step 3: Run focused validation serially**

```bash
perl -e 'alarm 300; exec @ARGV' uv run pytest -q -x tests/core/test_storage_billing.py tests/routers/test_admin_ops_and_storage.py -k 'storage'
cd compute/cloud
perl -e 'alarm 300; exec @ARGV' cargo test --test ingest storage_reconcile -- --test-threads=1
cd ../../frontend
perl -e 'alarm 420; exec @ARGV' npm test -- --maxWorkers=1 lib/cloud-engine/usage/storageBilling.test.ts lib/cloud-engine/usage/summary.test.ts lib/cloud-engine/adapter.test.ts __tests__/app/usage-platform.test.tsx __tests__/app/usage-sessions-streaming.test.tsx
perl -e 'alarm 300; exec @ARGV' npm run typecheck
perl -e 'alarm 180; exec @ARGV' npx eslint lib/cloud-engine/usage/storageBilling.ts lib/cloud-engine/usage/summary.ts lib/cloud-engine/adapter.ts app/usage/_sections/UsageClient.tsx app/usage/_sections/UsageStatCards.tsx components/CostCalculator/CostCalculator.tsx components/CostCalculator/CloudCostCalculator.tsx
```

Expected: every command passes without warnings or suppressed failures.

- [ ] **Step 4: Record the live gate**

Append to `notes/WIP.md`: the exact product SHA, Cloud local live/deleted interval totals, High-Scale local live/deleted interval totals, selected-period GB-hours, Platform account-wide value, and proof that the UI local cost did not change when Platform storage changed.

Add a read-only Playwright measurement with a warm, current inventory snapshot. Require navigation-to-storage-settled time `<= 20,000 ms`; if it exceeds that bound, inspect endpoint/request waterfall rather than increasing the goal.

- [ ] **Step 5: Commit**

```bash
git add frontend/openapi.json frontend/types/api.generated.ts frontend/lib/cloud-engine/cloud-api.generated.ts docs/pages/usage.md
git commit -m "Document local storage billing contract"
```
