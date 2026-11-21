# Cloud Manifest Startup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reuse a validated, identity-scoped Cloud manifest after a hard reload so a valid catalog generation advances with a delta instead of repeating a complete `/api/files` listing.

**Architecture:** Add a small CacheStorage record module around complete raw-request manifests. `catalog-manifest.ts` hydrates that record as its incremental base, still completes the generation delta before returning it, and persists only complete versioned results. Clear, resync, malformed-data, quota, and CacheStorage failures retain the existing network fallback.

**Tech Stack:** TypeScript, Vitest, browser CacheStorage, existing `catalog-manifest.ts` generation/delta machinery, `file-store.ts` storage error telemetry.

## Global Constraints

- Reuse only a complete, generation-scoped manifest for the same browser cache scope, service, signed-in identity, dataset, and view.
- Never begin Parquet reads from a hydrated record before the generation delta completes.
- Persist only complete raw-request manifests under the approved 4 MiB metadata and existing identity limits.
- Expire persisted manifest records after 24 hours.
- CacheStorage failure must fall back to the current complete-listing behavior.
- Do not add dependencies or change `/api/files` server semantics.
- Keep the existing two-second in-memory reuse, concurrent request coalescing, delta, resync, and closed-hour segment cache behavior.

---

### Task 1: Add the persisted manifest record boundary

**Files:**
- Create: `frontend/lib/cloud-engine/dashboard/manifest-store.ts`
- Create: `frontend/lib/cloud-engine/dashboard/manifest-store.test.ts`

**Interfaces:**
- Consumes: `Manifest`, `MAX_MANIFEST_BYTES`, `MAX_IDENTITIES`, `ManifestBudget`, `accountManifestPage`, `StoreError`.
- Produces:
  - `MANIFEST_SNAPSHOT_CACHE`, `MANIFEST_SNAPSHOT_MAX_AGE_MS`
  - `ManifestSnapshot`
  - `manifestSnapshotUrl(cacheKey)`
  - `readManifestSnapshot(cacheKey, now?)`
  - `persistManifestSnapshot(cacheKey, snapshot)`
  - `deleteManifestSnapshot(cacheKey)`
  - `deleteManifestSnapshots()`

- [ ] **Step 1: Write the failing record round-trip and validation tests**

Create `manifest-store.test.ts` with an in-memory CacheStorage fake and these behaviors:

```ts
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  MANIFEST_SNAPSHOT_MAX_AGE_MS,
  deleteManifestSnapshot,
  manifestSnapshotUrl,
  persistManifestSnapshot,
  readManifestSnapshot,
  type ManifestSnapshot,
} from "./manifest-store";

const scope = '["https://example.test","session","service"]';
const cacheKey = JSON.stringify([scope, "request", "raw", false]);
const from = "2026-01-10T00:00:00.000Z";
const to = "2026-01-11T00:00:00.000Z";

function snapshot(): ManifestSnapshot {
  return {
    from,
    to,
    generation: 7,
    manifest: {
      files: [{ key: "f1", start: from, end: to, size: 10, rows: 1 }],
      complete: true,
      catalogGeneration: 7,
    },
  };
}

function fakeCaches(initial: Record<string, Response> = {}) {
  const entries = new Map(Object.entries(initial));
  const cache = {
    match: vi.fn(async (url: string) => entries.get(url)?.clone() ?? undefined),
    put: vi.fn(async (url: string, response: Response) => {
      entries.set(url, response.clone());
    }),
    delete: vi.fn(async (url: string) => entries.delete(url)),
  } as unknown as Cache;
  vi.stubGlobal("caches", { open: vi.fn(async () => cache) });
  return { cache, entries };
}

afterEach(() => vi.unstubAllGlobals());

describe("manifest snapshots", () => {
  it("round-trips a complete scoped generation snapshot", async () => {
    fakeCaches();
    await persistManifestSnapshot(cacheKey, snapshot());
    expect(await readManifestSnapshot(cacheKey, Date.now())).toEqual(snapshot());
  });

  it("rejects an expired, wrong-scope, incomplete, or oversized record", async () => {
    const original = snapshot();
    const { entries, cache } = fakeCaches();
    await persistManifestSnapshot(cacheKey, original);
    const url = [...entries.keys()][0];
    expect(
      await readManifestSnapshot(
        cacheKey,
        Date.now() + MANIFEST_SNAPSHOT_MAX_AGE_MS,
      ),
    ).toBeNull();
    entries.set(
      url,
      new Response(
        JSON.stringify({
          formatVersion: 1,
          cacheKey: "other-scope",
          savedAt: Date.now(),
          snapshot: original,
        }),
      ),
    );
    expect(await readManifestSnapshot(cacheKey, Date.now())).toBeNull();
    entries.set(
      url,
      new Response(
        JSON.stringify({
          formatVersion: 1,
          cacheKey,
          savedAt: Date.now(),
          snapshot: {
            ...original,
            manifest: { ...original.manifest, complete: false },
          },
        }),
      ),
    );
    expect(await readManifestSnapshot(cacheKey, Date.now())).toBeNull();
    entries.set(
      url,
      new Response(
        JSON.stringify({
          formatVersion: 1,
          cacheKey,
          savedAt: Date.now(),
          snapshot: {
            ...original,
            manifest: {
              ...original.manifest,
              files: [{ ...original.manifest.files[0], key: "x".repeat(4 * 1024 * 1024) }],
            },
          },
        }),
      ),
    );
    expect(await readManifestSnapshot(cacheKey, Date.now())).toBeNull();
    expect(cache.delete).toHaveBeenCalled();
  });

  it("deletes only the selected scoped record", async () => {
    const { entries } = fakeCaches();
    await persistManifestSnapshot(cacheKey, snapshot());
    expect(entries.size).toBe(1);
    await deleteManifestSnapshot(cacheKey);
    expect(entries.size).toBe(0);
  });
});
```

- [ ] **Step 2: Run the focused test to prove the missing boundary fails**

Run:

```bash
cd /Users/drew.michael/Projects/fastly-log-analytics
perl -e 'alarm 120; exec @ARGV' npm exec -- vitest run frontend/lib/cloud-engine/dashboard/manifest-store.test.ts --maxWorkers=1
```

Expected: FAIL because `manifest-store.ts` does not exist.

- [ ] **Step 3: Implement the record module**

Create `manifest-store.ts` with the following shape. Keep parsing synchronous after a bounded response read; do not export the parser.

```ts
import {
  accountManifestPage,
  MAX_IDENTITIES,
  MAX_MANIFEST_BYTES,
  type ManifestBudget,
} from "./catalog-manifest";
import {
  describeError,
  recordStoreError,
  storageEstimate,
} from "./file-store";
import type { Manifest } from "./manifest";

export const MANIFEST_SNAPSHOT_CACHE = "flp-manifest-v1";
export const MANIFEST_SNAPSHOT_MAX_AGE_MS = 24 * 60 * 60 * 1000;
const FORMAT_VERSION = 1;

export type ManifestSnapshot = {
  from: string;
  to: string;
  generation: number;
  manifest: Manifest;
};

type PersistedManifest = {
  formatVersion: number;
  cacheKey: string;
  savedAt: number;
  snapshot: ManifestSnapshot;
};

export function manifestSnapshotUrl(cacheKey: string): string {
  return `/__flp_manifest_snapshot?v=${FORMAT_VERSION}&scope=${encodeURIComponent(cacheKey)}`;
}

function validIso(value: unknown): value is string {
  return typeof value === "string" && Number.isFinite(Date.parse(value));
}

function parseSnapshot(
  body: unknown,
  cacheKey: string,
  now: number,
): ManifestSnapshot | null {
  const record = body as Partial<PersistedManifest>;
  if (
    !record ||
    record.formatVersion !== FORMAT_VERSION ||
    record.cacheKey !== cacheKey ||
    typeof record.savedAt !== "number" ||
    record.savedAt > now ||
    now - record.savedAt >= MANIFEST_SNAPSHOT_MAX_AGE_MS
  )
    return null;
  const snapshot = record.snapshot as Partial<ManifestSnapshot> | undefined;
  const manifest = snapshot?.manifest as Partial<Manifest> | undefined;
  if (
    !snapshot ||
    !validIso(snapshot.from) ||
    !validIso(snapshot.to) ||
    Date.parse(snapshot.from) >= Date.parse(snapshot.to) ||
    !Number.isSafeInteger(snapshot.generation) ||
    snapshot.generation! < 0 ||
    !manifest ||
    manifest.complete !== true ||
    manifest.catalogGeneration !== snapshot.generation ||
    !Array.isArray(manifest.files)
  )
    return null;
  const budget: ManifestBudget = { bytes: 0, identities: new Set() };
  try {
    accountManifestPage({ files: manifest.files }, budget);
  } catch {
    return null;
  }
  if (
    budget.bytes > MAX_MANIFEST_BYTES ||
    budget.identities.size > MAX_IDENTITIES
  )
    return null;
  for (const file of manifest.files) {
    if (
      !validIso(file.start) ||
      !validIso(file.end) ||
      Date.parse(file.start) >= Date.parse(file.end) ||
      Date.parse(file.end) <= Date.parse(snapshot.from) ||
      Date.parse(file.start) >= Date.parse(snapshot.to)
    )
      return null;
  }
  if (manifest.summaryFiles !== undefined || manifest.summaryRange !== undefined)
    return null;
  return snapshot as ManifestSnapshot;
}

async function report(action: string, cacheKey: string, bytes: number, error: unknown) {
  const estimate = await storageEstimate();
  await recordStoreError({
    at: Date.now(),
    cache: MANIFEST_SNAPSHOT_CACHE,
    key: `manifest:${action}:${cacheKey}`,
    bytes,
    message: describeError(error),
    usage: estimate.usage,
    quota: estimate.quota,
  });
}

export async function readManifestSnapshot(
  cacheKey: string,
  now = Date.now(),
): Promise<ManifestSnapshot | null> {
  let bytes = 0;
  try {
    if (typeof caches === "undefined") return null;
    const cache = await caches.open(MANIFEST_SNAPSHOT_CACHE);
    const url = manifestSnapshotUrl(cacheKey);
    const response = await cache.match(url);
    if (!response) return null;
    const raw = await response.arrayBuffer();
    bytes = raw.byteLength;
    const body = raw.byteLength > MAX_MANIFEST_BYTES
      ? null
      : JSON.parse(new TextDecoder().decode(raw));
    const snapshot = parseSnapshot(body, cacheKey, now);
    if (!snapshot) await cache.delete(url);
    return snapshot;
  } catch (error) {
    await report("read", cacheKey, bytes, error);
    return null;
  }
}

export async function persistManifestSnapshot(
  cacheKey: string,
  snapshot: ManifestSnapshot,
): Promise<void> {
  let bytes = 0;
  try {
    if (!parseSnapshot({
      formatVersion: FORMAT_VERSION,
      cacheKey,
      savedAt: Date.now(),
      snapshot,
    }, cacheKey, Date.now()))
      throw new Error("Refusing to persist an invalid file manifest snapshot");
    const response = new Response(JSON.stringify({
      formatVersion: FORMAT_VERSION,
      cacheKey,
      savedAt: Date.now(),
      snapshot,
    } satisfies PersistedManifest));
    const clone = await response.clone().arrayBuffer();
    bytes = clone.byteLength;
    if (bytes > MAX_MANIFEST_BYTES)
      throw new Error("File manifest exceeded the metadata byte limit");
    const cache = await caches.open(MANIFEST_SNAPSHOT_CACHE);
    await cache.put(manifestSnapshotUrl(cacheKey), response);
  } catch (error) {
    await report("write", cacheKey, bytes, error);
  }
}

export async function deleteManifestSnapshot(cacheKey: string): Promise<void> {
  try {
    if (typeof caches === "undefined") return;
    const cache = await caches.open(MANIFEST_SNAPSHOT_CACHE);
    await cache.delete(manifestSnapshotUrl(cacheKey));
  } catch (error) {
    await report("delete", cacheKey, 0, error);
  }
}

export async function deleteManifestSnapshots(): Promise<void> {
  try {
    if (typeof caches === "undefined") return;
    await caches.delete(MANIFEST_SNAPSHOT_CACHE);
  } catch (error) {
    await report("clear", "*", 0, error);
  }
}
```

Export `MAX_IDENTITIES` from `catalog-manifest.ts` as part of this task instead of duplicating its value.

- [ ] **Step 4: Run the focused test and typecheck the new module**

Run:

```bash
perl -e 'alarm 120; exec @ARGV' npm exec -- vitest run frontend/lib/cloud-engine/dashboard/manifest-store.test.ts --maxWorkers=1
perl -e 'alarm 300; exec @ARGV' npm exec -- tsc --noEmit
```

Expected: PASS for the focused test and no TypeScript errors.

- [ ] **Step 5: Commit the record layer**

```bash
git add frontend/lib/cloud-engine/dashboard/manifest-store.ts frontend/lib/cloud-engine/dashboard/manifest-store.test.ts frontend/lib/cloud-engine/dashboard/catalog-manifest.ts
git commit -m "Add persisted Cloud manifest snapshots" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

### Task 2: Hydrate persisted snapshots and prove the no-relisting path

**Files:**
- Modify: `frontend/lib/cloud-engine/dashboard/catalog-manifest.ts`
- Modify: `frontend/lib/cloud-engine/dashboard/manifest.test.ts`

**Interfaces:**
- Consumes: `ManifestSnapshot`, `readManifestSnapshot`, `persistManifestSnapshot`
- Produces: unchanged `fetchCatalogManifest(...) -> Promise<Manifest>` behavior plus persisted-base hydration.

- [ ] **Step 1: Add a hard-reload-style failing integration test**

In `manifest.test.ts`, add this reusable test cache and clean it after each case:

```ts
function fakeManifestCacheStorage() {
  const entries = new Map<string, Response>();
  const cache = {
    match: vi.fn(async (url: string) => entries.get(url)?.clone()),
    put: vi.fn(async (url: string, value: Response) => {
      entries.set(url, value.clone());
    }),
    delete: vi.fn(async (url: string) => entries.delete(url)),
  } as unknown as Cache;
  const storage = { open: vi.fn(async () => cache), delete: vi.fn(async () => true) };
  vi.stubGlobal("caches", storage);
  return { cache, entries, storage };
}
```

Update the existing `afterEach` to call `vi.unstubAllGlobals()` after restoring real timers.

Add:

```ts
it("hydrates a complete persisted range and uses a delta instead of re-listing", async () => {
  fakeManifestCacheStorage();
  const original = file(T0);
  const entered = file(T0 + 2 * H);
  const calls: URLSearchParams[] = [];
  const fetchJson = Object.assign(vi.fn(async (input: string) => {
    const params = new URL(input, "http://x").searchParams;
    calls.push(params);
    if (!params.has("since_generation")) {
      return {
        files: [original],
        next_cursor: null,
        complete: true,
        catalog_generation: 7,
      };
    }
    expect(params.get("since_generation")).toBe("7");
    expect(params.get("previous_from")).toBe(iso(T0));
    expect(params.get("previous_to")).toBe(iso(T0 + 2 * H));
    return {
      files: [entered],
      changes: [],
      summary_partitions: [],
      base_generation: 7,
      catalog_generation: 8,
      next_cursor: null,
      complete: true,
    };
  }), {
    manifestCacheScope: () => '["session","service"]',
  });

  await fetchManifest(fetchJson, iso(T0), iso(T0 + 2 * H));
  vi.resetModules();
  const { fetchManifest: fetchAfterReload } = await import("./manifest");
  calls.length = 0;
  const result = await fetchAfterReload(
    fetchJson,
    iso(T0 + 60_000),
    iso(T0 + 2 * H + 60_000),
  );
  expect(calls).toHaveLength(1);
  expect(calls[0].get("since_generation")).toBe("7");
  expect(keys(result.files)).toEqual(keys([entered]));
});
```

The module reset simulates a new document: in-memory maps disappear, while the same fake CacheStorage object remains available.

- [ ] **Step 2: Run the new integration test to verify it fails**

Run:

```bash
perl -e 'alarm 120; exec @ARGV' npm exec -- vitest run frontend/lib/cloud-engine/dashboard/manifest.test.ts -t "hydrates a complete persisted range" --maxWorkers=1
```

Expected: FAIL because the second request performs a full listing without `since_generation`.

- [ ] **Step 3: Hydrate and install a persisted base**

Import `readManifestSnapshot` and `persistManifestSnapshot` into `catalog-manifest.ts`. In `fetchCatalogManifestUncached`, after computing `key`, `identity`, and `incremental`, hydrate only when no in-memory base exists:

```ts
let base = incremental ? snapshots.get(key) : undefined;
if (!base && incremental) {
  const saved = await readManifestSnapshot(key);
  if (saved) {
    base = {
      from: saved.from,
      to: saved.to,
      generation: saved.generation,
      manifest: saved.manifest,
    };
    snapshots.set(key, base);
  }
}
```

Do not return `base` directly. The existing `delta(...)` call must finish first.

- [ ] **Step 4: Persist only complete versioned incremental results**

After the existing generation and cache-identity checks, save the result before returning:

```ts
if (incremental && generation !== undefined && result.complete) {
  const snapshot = {
    from,
    to,
    generation,
    manifest: result,
  };
  const current = snapshots.get(key);
  if (!current || generation >= current.generation || resynced) {
    snapshots.delete(key);
    snapshots.set(key, snapshot);
    while (snapshots.size > MAX_SNAPSHOTS)
      snapshots.delete(snapshots.keys().next().value!);
    await persistManifestSnapshot(key, snapshot);
  }
}
```

If `persistManifestSnapshot` records an internal storage error, it resolves rather than throwing. Keep the returned `Manifest` unchanged.

- [ ] **Step 5: Run all manifest tests**

Run:

```bash
perl -e 'alarm 180; exec @ARGV' npm exec -- vitest run frontend/lib/cloud-engine/dashboard/manifest.test.ts frontend/lib/cloud-engine/dashboard/manifest-store.test.ts --maxWorkers=1
```

Expected: PASS, including the new incremental reload test and all existing pagination, closed-hour, cancellation, generation, and resync behavior.

- [ ] **Step 6: Commit hydration integration**

```bash
git add frontend/lib/cloud-engine/dashboard/catalog-manifest.ts frontend/lib/cloud-engine/dashboard/manifest.test.ts
git commit -m "Hydrate Cloud manifests across reloads" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

### Task 3: Cover invalidation, resync, and storage failures

**Files:**
- Modify: `frontend/lib/cloud-engine/dashboard/catalog-manifest.ts`
- Modify: `frontend/lib/cloud-engine/dashboard/manifest-store.ts`
- Modify: `frontend/lib/cloud-engine/dashboard/manifest.test.ts`
- Modify: `frontend/lib/cloud-engine/dashboard/manifest-store.test.ts`

**Interfaces:**
- Consumes: `deleteManifestSnapshot`, `deleteManifestSnapshots`, `recordStoreError`
- Produces: the same public cache-clear and commit-observation API with persistent invalidation.

- [ ] **Step 1: Add failing invalidation and fallback tests**

Add table-driven store tests:

```ts
it.each([
  ["malformed JSON", "{not-json"],
  ["wrong format", JSON.stringify({ formatVersion: 2 })],
  ["unsafe generation", JSON.stringify({
    formatVersion: 1,
    cacheKey,
    savedAt: Date.now(),
    snapshot: { ...snapshot(), generation: Number.MAX_SAFE_INTEGER + 1 },
  })],
  ["generation mismatch", JSON.stringify({
    formatVersion: 1,
    cacheKey,
    savedAt: Date.now(),
    snapshot: {
      ...snapshot(),
      manifest: { ...snapshot().manifest, catalogGeneration: 8 },
    },
  })],
])("falls back for %s persisted data", async (_name, body) => {
  const entries = new Map([[manifestSnapshotUrl(cacheKey), new Response(body)]]);
  fakeCaches(Object.fromEntries(entries));
  await expect(readManifestSnapshot(cacheKey, Date.now())).resolves.toBeNull();
});
```

Add `ManifestRequestError` and `persistManifestSnapshot` to the `manifest.test.ts` imports. Add integration tests proving:

```ts
it("clears persisted snapshots on an explicit manifest cache clear", async () => {
  const { storage } = fakeManifestCacheStorage();
  const state = { files: [file(T0)], gen: 1, before: T0 };
  const { fetchJson, calls } = server(state);
  await fetchManifest(fetchJson, iso(T0), iso(T0 + H));
  clearManifestCache();
  await fetchManifest(fetchJson, iso(T0), iso(T0 + H));
  expect(storage.delete).toHaveBeenCalled();
  expect(calls).toEqual([iso(T0), iso(T0)]);
});

it("replaces a stale persisted base after catalog resync", async () => {
  fakeManifestCacheStorage();
  const scope = '["persisted-resync","service"]';
  const cacheKey = JSON.stringify([scope, "request", "raw", false]);
  await persistManifestSnapshot(cacheKey, {
    from: iso(T0),
    to: iso(T0 + 2 * H),
    generation: 7,
    manifest: {
      files: [file(T0)],
      complete: true,
      catalogGeneration: 7,
    },
  });
  let fullLists = 0;
  let deltaLists = 0;
  const fetchJson = Object.assign(vi.fn(async (input: string) => {
    const params = new URL(input, "http://x").searchParams;
    if (!params.has("since_generation")) {
      fullLists++;
      return {
        files: [file(T0), file(T0 + H)],
        next_cursor: null,
        complete: true,
        catalog_generation: 9,
      };
    }
    deltaLists++;
    if (deltaLists === 1) {
      throw new ManifestRequestError(409, "catalog_resync_required");
    }
    return {
      files: [],
      changes: [],
      summary_partitions: [],
      base_generation: 9,
      catalog_generation: 9,
      next_cursor: null,
      complete: true,
    };
  }), {
    manifestCacheScope: () => scope,
  });
  vi.resetModules();
  const { fetchManifest: fetchAfterReload } = await import("./manifest");
  const current = await fetchAfterReload(
    fetchJson,
    iso(T0),
    iso(T0 + 2 * H),
  );
  expect(current.catalogGeneration).toBe(9);
  expect(deltaLists).toBe(1);
  expect(fullLists).toBe(1);
  vi.resetModules();
  const { fetchManifest: fetchAfterSecondReload } = await import("./manifest");
  await fetchAfterSecondReload(fetchJson, iso(T0), iso(T0 + 2 * H));
  expect(deltaLists).toBe(2);
  expect(fullLists).toBe(1);
});

it("falls back to a full listing when CacheStorage read rejects", async () => {
  vi.stubGlobal("caches", {
    open: vi.fn().mockRejectedValue(new DOMException("denied", "SecurityError")),
  });
  const { fetchJson, calls } = server({ files: [file(T0)], gen: 1, before: T0 });
  await expect(fetchManifest(fetchJson, iso(T0), iso(T0 + H))).resolves.toBeDefined();
  expect(calls).toEqual([iso(T0)]);
});
```

- [ ] **Step 2: Run the added tests to verify the missing behavior**

Run:

```bash
perl -e 'alarm 180; exec @ARGV' npm exec -- vitest run frontend/lib/cloud-engine/dashboard/manifest.test.ts frontend/lib/cloud-engine/dashboard/manifest-store.test.ts --maxWorkers=1
```

Expected: FAIL for persistent invalidation or CacheStorage fallback not yet wired.

- [ ] **Step 3: Wire persistent invalidation**

Keep `clearCatalogManifests()` synchronous. Track its deletion promise:

```ts
let pendingStoreInvalidation: Promise<void> = Promise.resolve();

export function clearCatalogManifests(): void {
  inFlight.clear();
  recent.clear();
  snapshots.clear();
  requirements.clear();
  fetchIds = new WeakMap();
  epoch++;
  pendingStoreInvalidation = pendingStoreInvalidation
    .then(deleteManifestSnapshots);
}
```

At the beginning of `fetchCatalogManifest`, wait for `pendingStoreInvalidation` before reading persisted state. For `observeCatalogCommit` resync:

```ts
if (event.type === "resync") {
  snapshots.delete(key);
  pendingStoreInvalidation = pendingStoreInvalidation.then(() =>
    deleteManifestSnapshot(key),
  );
}
```

Use the raw-request `manifestCacheKey` for the affected identity. Never delete another service's or identity's scoped snapshot.

- [ ] **Step 4: Record storage failures without changing network behavior**

Ensure every `readManifestSnapshot`, `persistManifestSnapshot`, `deleteManifestSnapshot`, and `deleteManifestSnapshots` rejection is converted into `recordStoreError` and resolves. If `recordStoreError` itself rejects, preserve a console warning and still resolve. Do not wrap or replace network `/api/files` errors.

- [ ] **Step 5: Run focused tests, frontend checks, and the product's secret-safe lint hooks**

Run:

```bash
perl -e 'alarm 180; exec @ARGV' npm exec -- vitest run frontend/lib/cloud-engine/dashboard/manifest.test.ts frontend/lib/cloud-engine/dashboard/manifest-store.test.ts frontend/lib/cloud-engine/dashboard/engine.test.ts --maxWorkers=1
perl -e 'alarm 300; exec @ARGV' npm exec -- eslint frontend/lib/cloud-engine/dashboard/manifest.ts frontend/lib/cloud-engine/dashboard/catalog-manifest.ts frontend/lib/cloud-engine/dashboard/manifest-store.ts frontend/lib/cloud-engine/dashboard/manifest.test.ts frontend/lib/cloud-engine/dashboard/manifest-store.test.ts
perl -e 'alarm 300; exec @ARGV' npm exec -- tsc --noEmit
git diff --check
```

Expected: all commands exit 0. The tests prove zero complete listing on a valid warm reload, generation-delta freshness, and fallback on invalid storage.

- [ ] **Step 6: Commit the invalidation and fallback behavior**

```bash
git add frontend/lib/cloud-engine/dashboard/catalog-manifest.ts frontend/lib/cloud-engine/dashboard/manifest-store.ts frontend/lib/cloud-engine/dashboard/manifest.test.ts frontend/lib/cloud-engine/dashboard/manifest-store.test.ts
git commit -m "Harden persisted Cloud manifest recovery" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
```

### Task 4: Review and update the live verification record

**Files:**
- Modify: no product implementation files
- Review: `runs/flp-cloud/*/report.md` only after an explicitly approved future combined live run

- [ ] **Step 1: Inspect the final diff for the spec's acceptance criteria**

Run:

```bash
git status --short
git diff origin/v3.0.0-rc3...HEAD -- frontend/lib/cloud-engine/dashboard
```

Verify:

- valid persisted startup performs an incremental delta and no full listing;
- Parquet consumers still wait for a complete delta;
- resync and identity changes cannot return another scope's record;
- no secret, token, identity, or full manifest content is logged; and
- no non-request dataset is persisted.

- [ ] **Step 2: Update the task status without claiming live proof**

Mark `cloud-manifest-startup` done only after the focused tests and checks pass. Record in the deploy-testing `notes/WIP.md` that live evidence still requires the next explicitly approved combined three-environment run.

- [ ] **Step 3: Stop before live traffic**

Do not deploy or generate load. The next combined live run requires a new explicit approval for that exact product SHA and effects under the repository protocol.
