export type ManifestFile = {
  key: string;
  start: string;
  end: string;
  size: number;
  rows: number;
  etag?: string;
  schema_version?: string | number;
  /** view=summary only. */
  kind?: "day" | "hour" | "window";
  sources?: number;
};

export type Manifest = {
  files: ManifestFile[];
  complete: boolean;
  catalogGeneration?: number;
  /** Inline summary listing returned for `summaryRange` when requested. */
  summaryFiles?: ManifestFile[];
  summaryRange?: { from: string; to: string };
};

/** Which closed hours of a raw request listing a browser may keep, and under which generation. */
type CacheHint = { gen: number; from: string; before: string };

type ManifestPage = {
  files: ManifestFile[];
  next_cursor: string | null;
  complete: boolean;
  cache?: CacheHint | null;
  summary_files?: ManifestFile[];
  catalog_generation?: number | string;
};

export type ListView = "raw" | "summary";

export type FetchJson = ((
  url: string,
  signal?: AbortSignal,
) => Promise<unknown>) & {
  manifestCacheScope?: () => string;
};

const MAX_PAGES = 64;
const HOUR_MS = 3_600_000;
const SEGMENT_MAX_AGE_MS = HOUR_MS;

/**
 * The raw request listing of `[from, to)`, closed hours the server marked
 * immutable under generation `gen`. Each file overlaps the range.
 */
type Segment = {
  scopeKey: string;
  gen: number;
  from: number;
  to: number;
  savedAt: number;
  files: ManifestFile[];
};

let segment: Segment | null = null;

/** Invalidates closed hours too: a committed late arrival can change them. */
export function clearManifestCache(): void {
  segment = null;
  clearCatalogManifests();
}

export function observeManifestCommit(
  fetchJson: FetchJson,
  event: {
    type: "ready" | "commit" | "resync";
    generation: number;
    manifest?: string | null;
  },
): void {
  segment = null;
  observeCatalogCommit(fetchJson, event);
}

/**
 * Call before discovery after every commit/resync notification, including a
 * late batch in a closed hour. This leaves decoded summaries and rollups intact.
 */
export const invalidateManifestCache = clearManifestCache;

type Listing = Manifest & { hint: CacheHint | null };

async function listPages(
  fetchJson: FetchJson,
  from: string,
  to: string,
  signal: AbortSignal | undefined,
  dataset: string | undefined,
  view: ListView | undefined,
  includeSummaries?: boolean,
  budget?: ManifestBudget,
): Promise<Listing> {
  const files = new Map<string, ManifestFile>();
  let cursor: string | null = null;
  let hint: CacheHint | null | undefined;
  const summaryFiles = new Map<string, ManifestFile>();
  let summariesIncluded = false;
  let catalogGeneration: number | string | undefined;
  const seen = new Set<string>();
  for (let page = 0; page < MAX_PAGES; page++) {
    const params = new URLSearchParams({ from, to });
    if (dataset) params.set("dataset", dataset);
    if (view) params.set("view", view);
    if (includeSummaries) params.set("summaries", "1");
    if (cursor) params.set("cursor", cursor);
    const body = (await fetchJson(
      `/api/files?${params}`,
      signal,
    )) as ManifestPage;
    signal?.throwIfAborted();
    if (budget) accountManifestPage(body, budget);
    if (page === 0) catalogGeneration = body.catalog_generation;
    else if (body.catalog_generation !== catalogGeneration) {
      throw new Error(
        "File manifest catalog generation changed during pagination",
      );
    }
    for (const file of body.files) files.set(file.key, file);
    if (Array.isArray(body.summary_files)) {
      summariesIncluded = true;
      for (const file of body.summary_files) summaryFiles.set(file.key, file);
    }
    // Pages that disagree on the generation straddled a commit.
    const pageHint = body.cache ?? null;
    hint =
      hint === undefined
        ? pageHint
        : hint && pageHint && hint.gen === pageHint.gen
          ? hint
          : null;
    cursor = body.next_cursor;
    if (!cursor) {
      return {
        files: [...files.values()],
        complete: body.complete,
        hint: hint ?? null,
        ...(catalogGeneration !== undefined
          ? { catalogGeneration: manifestGeneration(catalogGeneration) }
          : {}),
        ...(summariesIncluded
          ? {
              summaryFiles: [...summaryFiles.values()],
              summaryRange: { from, to },
            }
          : {}),
      };
    }
    if (seen.has(cursor)) throw new Error("File manifest cursor repeated");
    seen.add(cursor);
  }
  throw new Error("File manifest exceeded the page limit");
}

/** Keeps `files` (a complete listing of at least `[fromMs, toMs)`) up to the last closed hour. */
function remember(
  hint: CacheHint | null,
  files: ManifestFile[],
  fromMs: number,
  toMs: number,
  scopeKey: string,
): void {
  if (!hint) return;
  const end = Math.min(
    Math.floor(Date.parse(hint.before) / HOUR_MS) * HOUR_MS,
    Math.floor(toMs / HOUR_MS) * HOUR_MS,
  );
  if (!(end > fromMs) || fromMs < Date.parse(hint.from)) return;
  segment = {
    scopeKey,
    gen: hint.gen,
    from: fromMs,
    to: end,
    savedAt: Date.now(),
    files: filesOverlapping(files, fromMs, end),
  };
}

/**
 * Lists `/api/files` for `[from, to)`. A raw request listing keeps the closed
 * hours the server marks immutable; a later listing that starts inside them
 * re-lists only from their end and reuses them while the generation holds.
 */
async function fetchSnapshot(
  fetchJson: FetchJson,
  from: string,
  to: string,
  signal?: AbortSignal,
  dataset?: string,
  view?: ListView,
  includeSummaries?: boolean,
  budget?: ManifestBudget,
  fresh = false,
): Promise<Manifest> {
  const cacheable =
    (!dataset || dataset === "request") && (!view || view === "raw");
  if (!cacheable) {
    const listing = await listPages(
      fetchJson,
      from,
      to,
      signal,
      dataset,
      view,
      includeSummaries,
      budget,
    );
    return {
      files: listing.files,
      complete: listing.complete,
      ...(listing.catalogGeneration !== undefined
        ? { catalogGeneration: listing.catalogGeneration }
        : {}),
      ...(listing.summaryFiles
        ? {
            summaryFiles: listing.summaryFiles,
            summaryRange: listing.summaryRange,
          }
        : {}),
    };
  }
  const fromMs = Date.parse(from);
  const toMs = Date.parse(to);
  const scopeKey = manifestCacheKey(fetchJson, dataset, view, includeSummaries);
  const kept = segment;
  if (
    !includeSummaries &&
    !fresh &&
    kept &&
    kept.scopeKey === scopeKey &&
    Date.now() - kept.savedAt < SEGMENT_MAX_AGE_MS &&
    kept.from <= fromMs &&
    fromMs < kept.to &&
    kept.to < toMs
  ) {
    const tail = await listPages(
      fetchJson,
      new Date(kept.to).toISOString(),
      to,
      signal,
      dataset,
      view,
      undefined,
      budget,
    );
    if (
      tail.hint &&
      tail.hint.gen === kept.gen &&
      kept.from >= Date.parse(tail.hint.from)
    ) {
      const files = new Map<string, ManifestFile>();
      for (const file of filesOverlapping(kept.files, fromMs, kept.to)) {
        files.set(file.key, file);
      }
      for (const file of tail.files) files.set(file.key, file);
      const merged = [...files.values()];
      if (tail.complete) {
        remember({ ...tail.hint }, merged, fromMs, toMs, scopeKey);
      }
      return { files: merged, complete: tail.complete };
    }
  }
  const listing = await listPages(
    fetchJson,
    from,
    to,
    signal,
    dataset,
    view,
    includeSummaries,
    budget,
  );
  if (listing.complete)
    remember(listing.hint, listing.files, fromMs, toMs, scopeKey);
  return {
    files: listing.files,
    complete: listing.complete,
    ...(listing.catalogGeneration !== undefined
      ? { catalogGeneration: listing.catalogGeneration }
      : {}),
    ...(listing.summaryFiles
      ? {
          summaryFiles: listing.summaryFiles,
          summaryRange: listing.summaryRange,
        }
      : {}),
  };
}

export async function fetchManifest(
  fetchJson: FetchJson,
  from: string,
  to: string,
  signal?: AbortSignal,
  dataset?: string,
  view?: ListView,
  includeSummaries?: boolean,
): Promise<Manifest> {
  return fetchCatalogManifest(
    fetchJson,
    from,
    to,
    signal,
    dataset,
    view,
    includeSummaries,
    (budget, fresh) =>
      fetchSnapshot(
        fetchJson,
        from,
        to,
        signal,
        dataset,
        view,
        includeSummaries,
        budget,
        fresh,
      ),
  );
}

export function filesOverlapping(
  files: ManifestFile[],
  fromMs: number,
  toMs: number,
): ManifestFile[] {
  return files.filter(
    (file) => Date.parse(file.end) > fromMs && Date.parse(file.start) < toMs,
  );
}
import {
  accountManifestPage,
  clearCatalogManifests,
  fetchCatalogManifest,
  manifestCacheKey,
  manifestGeneration,
  observeCatalogCommit,
  type ManifestBudget,
} from "./catalog-manifest";
