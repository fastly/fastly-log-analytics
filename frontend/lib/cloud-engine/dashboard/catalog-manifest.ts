import type { FetchJson, Manifest, ManifestFile, ListView } from "./manifest";
import { CARD_FIELDS, type CustomCard } from "./summaries";

export const FIELD_GROUP_BY_COLUMN: Readonly<Record<string, string>> = Object.fromEntries(
  CARD_FIELDS.map((field) => [field, field]),
);

export const ALWAYS_AVAILABLE_FILTER_FIELDS: ReadonlySet<string> = new Set([
  ...CARD_FIELDS,
  "query",
  "service_id",
  "timestamp",
  "ts_ms",
  "time",
  "bytes",
]);

const CATALOG_KEY_RE = /(?:^|\/)catalog(\d+)\/([0-9a-f]{64})\//;

export function catalogHashesInManifest(files: ManifestFile[]): Set<string> {
  const hashes = new Set<string>();
  for (const f of files) {
    const m = CATALOG_KEY_RE.exec(f.key);
    if (m) hashes.add(m[2]);
  }
  return hashes;
}

export function hasLegacyUncatalogedFiles(files: ManifestFile[]): boolean {
  return files.some((f) => !CATALOG_KEY_RE.test(f.key));
}

export function filterUnavailableCustomFields<T extends CustomCard>(
  customFields: readonly T[],
  files?: ManifestFile[],
  availableFieldIds?: ReadonlySet<string>,
): T[] {
  if (!customFields.length) return [];
  if (!availableFieldIds) return [...customFields];
  if (files && files.length > 0 && hasLegacyUncatalogedFiles(files)) {
    return [...customFields];
  }
  return customFields.filter((f) => availableFieldIds.has(f.id));
}

export const MAX_MANIFEST_BYTES = 4 * 1024 * 1024;
const MAX_IDENTITIES = 16_384;
const MAX_REPLAY = 32;
const MAX_SNAPSHOTS = 8;
const MAX_DELTA_PAGES = 64;
const DAY_MS = 86_400_000;

export class ManifestRequestError extends Error {
  constructor(
    public readonly status: number,
    public readonly code?: string,
  ) {
    super(`Request failed (${status})${code ? `: ${code}` : ""}`);
  }
}

export type ManifestBudget = { bytes: number; identities: Set<string> };
type Snapshot = {
  from: string;
  to: string;
  generation: number;
  manifest: Manifest;
};
type Commit = { generation: number; files: ManifestFile[]; replaces: string[] };
type Partition = { from: string; to: string };
type DeltaPage = {
  files: ManifestFile[];
  summary_files?: ManifestFile[];
  changes: Commit[];
  summary_partitions: Partition[];
  base_generation: number;
  catalog_generation: number;
  next_cursor: string | null;
  complete: boolean;
};
type Requirement = {
  generation: number;
  reset: number;
  manifest?: string | null;
};
const snapshots = new Map<string, Snapshot>();
const requirements = new Map<string, Requirement>();
let fetchIds = new WeakMap<FetchJson, number>();
let nextFetchId = 0;
let epoch = 0;

function scope(fetchJson: FetchJson): string {
  if (fetchJson.manifestCacheScope) return fetchJson.manifestCacheScope();
  let id = fetchIds.get(fetchJson);
  if (id === undefined) {
    id = ++nextFetchId;
    fetchIds.set(fetchJson, id);
  }
  return `fetch:${id}`;
}

export function manifestCacheKey(
  fetchJson: FetchJson,
  dataset?: string,
  view?: ListView,
  summaries?: boolean,
): string {
  return JSON.stringify([
    scope(fetchJson),
    dataset ?? "request",
    view ?? "raw",
    Boolean(summaries),
  ]);
}

export function clearCatalogManifests(): void {
  inFlight.clear();
  snapshots.clear();
  requirements.clear();
  fetchIds = new WeakMap();
  epoch++;
}

export function observeCatalogCommit(
  fetchJson: FetchJson,
  event: {
    type: "ready" | "commit" | "resync";
    generation: number;
    manifest?: string | null;
  },
): void {
  const identity = scope(fetchJson);
  const previous = requirements.get(identity);
  if (event.type === "resync") {
    for (const key of snapshots.keys())
      if (JSON.parse(key)[0] === identity) snapshots.delete(key);
  }
  requirements.set(identity, {
    generation:
      event.type === "resync"
        ? event.generation
        : Math.max(previous?.generation ?? 0, event.generation),
    reset: (previous?.reset ?? 0) + (event.type === "resync" ? 1 : 0),
    manifest: event.manifest,
  });
}

export function manifestGeneration(value: unknown): number | undefined {
  if (value === undefined) return undefined;
  const number =
    typeof value === "string" && /^\d+$/.test(value) ? Number(value) : value;
  if (typeof number !== "number" || !Number.isSafeInteger(number) || number < 0)
    throw new Error("Invalid file manifest catalog generation");
  return number;
}

export function accountManifestPage(
  body: unknown,
  budget: ManifestBudget,
): void {
  budget.bytes += new TextEncoder().encode(JSON.stringify(body)).byteLength;
  if (budget.bytes > MAX_MANIFEST_BYTES)
    throw new Error("File manifest exceeded the metadata byte limit");
  const page = body as Partial<DeltaPage>;
  const add = (file: ManifestFile) => {
    if (!file || typeof file.key !== "string" || !file.key)
      throw new Error("Invalid file manifest identity");
    budget.identities.add(file.key);
  };
  for (const file of page.files ?? []) add(file);
  for (const file of page.summary_files ?? []) add(file);
  for (const change of page.changes ?? []) {
    for (const file of change.files ?? []) add(file);
    for (const key of change.replaces ?? []) {
      if (typeof key !== "string" || !key)
        throw new Error("Invalid file manifest replacement identity");
      budget.identities.add(key);
    }
  }
  if (budget.identities.size > MAX_IDENTITIES)
    throw new Error("File manifest exceeded the identity limit");
}

function overlaps(file: ManifestFile, from: string, to: string): boolean {
  const start = Date.parse(file.start);
  const end = Date.parse(file.end);
  return (
    !Number.isFinite(start) ||
    !Number.isFinite(end) ||
    (end > Date.parse(from) && start < Date.parse(to))
  );
}

async function delta(
  fetchJson: FetchJson,
  base: Snapshot,
  from: string,
  to: string,
  signal: AbortSignal | undefined,
  dataset: string | undefined,
  view: ListView | undefined,
  summaries: boolean | undefined,
  budget: ManifestBudget,
): Promise<Manifest> {
  const entered = new Map<string, ManifestFile>();
  const summariesAtTarget = new Map<string, ManifestFile>();
  const commits = new Map<
    number,
    { files: Map<string, ManifestFile>; replaces: Set<string> }
  >();
  const resetPartitions = new Map<string, Partition>();
  const seen = new Set<string>();
  let target: number | undefined;
  let cursor: string | null = null;
  for (let page = 0; page < MAX_DELTA_PAGES; page++) {
    signal?.throwIfAborted();
    const params = new URLSearchParams({
      from,
      to,
      since_generation: String(base.generation),
      previous_from: base.from,
      previous_to: base.to,
    });
    if (dataset) params.set("dataset", dataset);
    if (view) params.set("view", view);
    if (summaries) params.set("summaries", "1");
    if (cursor) params.set("cursor", cursor);
    const body = (await fetchJson(`/api/files?${params}`, signal)) as DeltaPage;
    signal?.throwIfAborted();
    accountManifestPage(body, budget);
    if (manifestGeneration(body.base_generation) !== base.generation)
      throw new Error("File manifest base generation changed");
    const generation = manifestGeneration(body.catalog_generation);
    if (generation === undefined)
      throw new Error("File manifest delta omitted its catalog generation");
    if (generation < base.generation)
      throw new Error("File manifest returned an older catalog generation");
    if (generation - base.generation > MAX_REPLAY)
      throw new Error("File manifest exceeded the replay limit");
    if (target !== undefined && target !== generation)
      throw new Error(
        "File manifest catalog generation changed during pagination",
      );
    target = generation;
    if (
      !Array.isArray(body.files) ||
      !Array.isArray(body.changes) ||
      !Array.isArray(body.summary_partitions)
    )
      throw new Error("Invalid incremental file manifest");
    for (const file of body.files) entered.set(file.key, file);
    for (const file of body.summary_files ?? [])
      summariesAtTarget.set(file.key, file);
    for (const partition of body.summary_partitions) {
      const start = Date.parse(partition.from);
      const end = Date.parse(partition.to);
      if (
        !Number.isFinite(start) ||
        !Number.isFinite(end) ||
        start % DAY_MS !== 0 ||
        end !== start + DAY_MS
      )
        throw new Error("Invalid summary reset partition");
      resetPartitions.set(partition.from, partition);
    }
    for (const change of body.changes) {
      const at = manifestGeneration(change.generation);
      if (
        at === undefined ||
        at <= base.generation ||
        at > target ||
        !Array.isArray(change.files) ||
        !Array.isArray(change.replaces)
      )
        throw new Error("Invalid file manifest change generation");
      let commit = commits.get(at);
      if (!commit) {
        commit = { files: new Map(), replaces: new Set() };
        commits.set(at, commit);
      }
      for (const key of change.replaces) commit.replaces.add(key);
      for (const file of change.files) commit.files.set(file.key, file);
    }
    cursor = body.next_cursor;
    if (!cursor) {
      if (body.complete !== true)
        throw new Error("Incremental file manifest is incomplete");
      break;
    }
    if (typeof cursor !== "string" || seen.has(cursor))
      throw new Error("File manifest cursor repeated or invalid");
    seen.add(cursor);
    if (page === MAX_DELTA_PAGES - 1)
      throw new Error("File manifest exceeded the page limit");
  }
  const files = new Map(base.manifest.files.map((file) => [file.key, file]));
  for (const [, commit] of [...commits].sort(([a], [b]) => a - b)) {
    for (const key of commit.replaces) files.delete(key);
    for (const [key, file] of commit.files) files.set(key, file);
  }
  for (const [key, file] of entered) files.set(key, file);
  const summaryFiles = new Map(
    (base.manifest.summaryFiles ?? []).map((file) => [file.key, file]),
  );
  const partitions = [...resetPartitions.values()];
  for (const [key, file] of summaryFiles) {
    if (
      partitions.some((partition) =>
        overlaps(file, partition.from, partition.to),
      )
    )
      summaryFiles.delete(key);
  }
  for (const [key, file] of summariesAtTarget) {
    if (
      !partitions.some((partition) =>
        overlaps(file, partition.from, partition.to),
      )
    )
      throw new Error(
        "Summary lies outside its authoritative reset partitions",
      );
    summaryFiles.set(key, file);
  }
  const retained = [...files.values()].filter((file) =>
    overlaps(file, from, to),
  );
  const retainedSummaries = [...summaryFiles.values()].filter((file) =>
    overlaps(file, from, to),
  );
  if (retained.length + retainedSummaries.length > MAX_IDENTITIES)
    throw new Error("File manifest exceeded the identity limit");
  return {
    files: retained,
    complete: true,
    catalogGeneration: target,
    ...(summaries
      ? { summaryFiles: retainedSummaries, summaryRange: { from, to } }
      : {}),
  };
}

type SharedListing = {
  promise: Promise<Manifest>;
  controller: AbortController;
  waiters: number;
  settled: boolean;
};

const inFlight = new Map<string, SharedListing>();

function isAbort(error: unknown): boolean {
  return (
    error instanceof Error &&
    (error.name === "AbortError" || error.name === "TimeoutError")
  );
}

function abortReason(signal: AbortSignal): unknown {
  return signal.reason ?? Object.assign(new Error("aborted"), { name: "AbortError" });
}

/**
 * Concurrent callers asking for the same listing share one request. A page
 * opens several panels at once; without this each one listed the range
 * itself before the first snapshot could be cached. The shared request runs
 * on its own signal and is aborted only once every caller has aborted, so a
 * panel that re-renders and cancels its query does not restart the listing
 * for the others.
 */
export async function fetchCatalogManifest(
  fetchJson: FetchJson,
  from: string,
  to: string,
  signal: AbortSignal | undefined,
  dataset: string | undefined,
  view: ListView | undefined,
  summaries: boolean | undefined,
  loadSnapshot: (
    budget: ManifestBudget,
    fresh: boolean,
    signal: AbortSignal,
  ) => Promise<Manifest>,
): Promise<Manifest> {
  const key = manifestCacheKey(fetchJson, dataset, view, summaries);
  const shareKey = `${key}|${from}|${to}`;
  signal?.throwIfAborted();
  const shared = inFlight.get(shareKey);
  if (shared && !shared.settled) {
    try {
      return await join(shared, signal);
    } catch (error) {
      signal?.throwIfAborted();
      if (!isAbort(error)) throw error;
    }
  }
  const controller = new AbortController();
  const own: SharedListing = {
    controller,
    waiters: 0,
    settled: false,
    promise: undefined as unknown as Promise<Manifest>,
  };
  own.promise = fetchCatalogManifestUncached(
    fetchJson,
    from,
    to,
    controller.signal,
    dataset,
    view,
    summaries,
    loadSnapshot,
  ).finally(() => {
    own.settled = true;
    if (inFlight.get(shareKey) === own) inFlight.delete(shareKey);
  });
  inFlight.set(shareKey, own);
  return join(own, signal);
}

/** Waits on a shared listing, leaving it when this caller's own signal aborts. */
function join(shared: SharedListing, signal: AbortSignal | undefined): Promise<Manifest> {
  shared.waiters++;
  let left = false;
  const leave = () => {
    if (left) return;
    left = true;
    if (--shared.waiters === 0 && !shared.settled) shared.controller.abort();
  };
  return new Promise<Manifest>((resolve, reject) => {
    const onAbort = () => {
      leave();
      reject(abortReason(signal!));
    };
    if (signal?.aborted) {
      onAbort();
      return;
    }
    signal?.addEventListener("abort", onAbort, { once: true });
    shared.promise.then(
      (value) => {
        signal?.removeEventListener("abort", onAbort);
        leave();
        resolve(value);
      },
      (error: unknown) => {
        signal?.removeEventListener("abort", onAbort);
        leave();
        reject(error);
      },
    );
  });
}

async function fetchCatalogManifestUncached(
  fetchJson: FetchJson,
  from: string,
  to: string,
  signal: AbortSignal,
  dataset: string | undefined,
  view: ListView | undefined,
  summaries: boolean | undefined,
  loadSnapshot: (
    budget: ManifestBudget,
    fresh: boolean,
    signal: AbortSignal,
  ) => Promise<Manifest>,
): Promise<Manifest> {
  const key = manifestCacheKey(fetchJson, dataset, view, summaries);
  const identity = scope(fetchJson);
  const incremental =
    (!dataset || dataset === "request") && (!view || view === "raw");
  const startEpoch = epoch;
  const reset = requirements.get(identity)?.reset ?? 0;
  const base = incremental ? snapshots.get(key) : undefined;
  const budget: ManifestBudget = { bytes: 0, identities: new Set() };
  let resynced = false;
  let result: Manifest;
  try {
    result = base
      ? await delta(
          fetchJson,
          base,
          from,
          to,
          signal,
          dataset,
          view,
          summaries,
          budget,
        )
      : await loadSnapshot(budget, false, signal);
  } catch (error) {
    if (
      !base ||
      !(
        error &&
        typeof error === "object" &&
        "status" in error &&
        error.status === 409 &&
        "code" in error &&
        error.code === "catalog_resync_required"
      )
    )
      throw error;
    signal?.throwIfAborted();
    result = await loadSnapshot(budget, true, signal);
    if (!result.complete || result.catalogGeneration === undefined)
      throw new Error("Catalog resync snapshot is incomplete or unversioned");
    resynced = true;
  }
  signal?.throwIfAborted();
  if (
    startEpoch !== epoch ||
    scope(fetchJson) !== identity ||
    (incremental && reset !== (requirements.get(identity)?.reset ?? 0))
  )
    throw new Error("File manifest cache identity changed during discovery");
  const generation = result.catalogGeneration;
  if (incremental && generation !== undefined && result.complete) {
    const minimum = requirements.get(identity)?.generation;
    if (minimum !== undefined && generation < minimum)
      throw new ManifestRequestError(503, "catalog_head_behind");
    const current = snapshots.get(key);
    if (!current || generation >= current.generation || resynced) {
      snapshots.delete(key);
      snapshots.set(key, { from, to, generation, manifest: result });
      while (snapshots.size > MAX_SNAPSHOTS)
        snapshots.delete(snapshots.keys().next().value!);
    }
  }
  return result;
}
