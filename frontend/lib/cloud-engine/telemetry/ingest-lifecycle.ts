import type { DecodedColumns } from "../dashboard/parquet";
import type {
  StorageBillingInterval,
  StorageBillingIntervalKind,
} from "../usage/storageBilling";
import type { LogsStats } from "./kinds";

const DAY_MS = 86_400_000;

export const INGEST_LIFECYCLE_COLUMNS = [
  "timestamp",
  "raw_key",
  "raw_size",
  "status",
  "state_version",
  "output_keys_json",
  "raw_deleted",
  "raw_deleted_at",
] as const;

export interface IngestOutput {
  key: string;
  bytes: number;
  createdAt: number;
}

export interface RawLifecycleState {
  earliestTs: number | null;
  sawNonDeleted: boolean;
  knownSize: number | undefined;
  latest: {
    deleted: boolean;
    deletedAt: number | null;
    rowAt: number;
    version: number;
  };
}

export interface IngestLifecycleFold {
  rowCount: number;
  minTs: number;
  maxTs: number;
  lifecycles: Map<string, RawLifecycleState>;
  outputs: Map<string, IngestOutput>;
  deletedParquetKeys: Set<string>;
  seenOutputJson: Set<string>;
  seenDeletedJson: Set<string>;
}

export interface IngestLifecycleResult {
  intervals: StorageBillingInterval[];
  outputs: IngestOutput[];
  deletedParquetKeys: string[];
  bytes: number;
  complete: boolean;
  provenance: "cloud:ingest_files" | "cloud:ingest_files_incomplete";
  liveTruncated?: boolean;
  stats?: LogsStats;
}

export function createIngestLifecycleFold(): IngestLifecycleFold {
  return {
    rowCount: 0,
    minTs: Infinity,
    maxTs: -Infinity,
    lifecycles: new Map(),
    outputs: new Map(),
    deletedParquetKeys: new Set(),
    seenOutputJson: new Set(),
    seenDeletedJson: new Set(),
  };
}

function parseTimestampMsValue(raw: unknown): number | null {
  if (raw === null || raw === undefined || raw === "") return null;
  if (typeof raw === "number") return Number.isFinite(raw) ? raw : null;
  if (typeof raw === "bigint") {
    const n = Number(raw);
    return Number.isFinite(n) ? n : null;
  }
  if (raw instanceof Date) {
    const ms = raw.getTime();
    return Number.isFinite(ms) ? ms : null;
  }
  const numeric = Number(raw);
  if (Number.isFinite(numeric)) return numeric;
  if (typeof raw === "string") {
    const parsed = Date.parse(raw);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function parseNumericValue(raw: unknown): number {
  if (raw === null || raw === undefined) return NaN;
  if (typeof raw === "number") return raw;
  if (typeof raw === "bigint") return Number(raw);
  return Number(raw);
}

function foldSingleRecord(
  fold: IngestLifecycleFold,
  rawTs: unknown,
  rawKeyVal: unknown,
  rawSizeVal: unknown,
  statusVal: unknown,
  stateVersionVal: unknown,
  outputKeysJsonVal: unknown,
  rawDeletedVal: unknown,
  rawDeletedAtVal: unknown,
  fromMs?: number,
  toMs?: number,
): void {
  const ts = parseTimestampMsValue(rawTs);
  if (fromMs !== undefined && toMs !== undefined) {
    if (ts === null || ts < fromMs || ts >= toMs) return;
  }
  if (ts !== null) {
    if (ts < fold.minTs) fold.minTs = ts;
    if (ts > fold.maxTs) fold.maxTs = ts;
  }

  if (
    (statusVal === "converted" || statusVal === "compacted") &&
    typeof outputKeysJsonVal === "string" &&
    outputKeysJsonVal.length > 2
  ) {
    if (ts !== null && !fold.seenOutputJson.has(outputKeysJsonVal)) {
      fold.seenOutputJson.add(outputKeysJsonVal);
      let written: unknown = [];
      try {
        written = JSON.parse(outputKeysJsonVal);
      } catch {
        written = [];
      }
      if (Array.isArray(written)) {
        for (let j = 0; j < written.length; j++) {
          const output = written[j];
          const key = typeof output?.key === "string" ? output.key : "";
          const bytes = Number(output?.bytes);
          if (!key || !Number.isFinite(bytes) || fold.outputs.has(key)) continue;
          fold.outputs.set(key, { key, bytes, createdAt: ts });
        }
      }
    }
  } else if (
    statusVal === "parquet_deleted" &&
    typeof outputKeysJsonVal === "string" &&
    outputKeysJsonVal.length > 2
  ) {
    if (!fold.seenDeletedJson.has(outputKeysJsonVal)) {
      fold.seenDeletedJson.add(outputKeysJsonVal);
      let deleted: unknown = [];
      try {
        deleted = JSON.parse(outputKeysJsonVal);
      } catch {
        deleted = [];
      }
      if (Array.isArray(deleted)) {
        for (let j = 0; j < deleted.length; j++) {
          const entry = deleted[j];
          const key =
            typeof entry === "string"
              ? entry
              : typeof entry?.key === "string"
                ? entry.key
                : "";
          if (key) fold.deletedParquetKeys.add(key);
        }
      }
    }
  }

  const rawKey = typeof rawKeyVal === "string" ? rawKeyVal : "";
  if (!rawKey) return;

  const size = parseNumericValue(rawSizeVal);
  const validSize = Number.isFinite(size) && size >= 0 ? size : undefined;
  const deletedAt = parseTimestampMsValue(rawDeletedAtVal);
  const rowAt = deletedAt ?? ts ?? 0;
  const version =
    stateVersionVal === null || stateVersionVal === undefined
      ? 0
      : parseNumericValue(stateVersionVal);
  const validVersion = Number.isFinite(version) ? version : 0;
  const deleted = rawDeletedVal === true;

  const existing = fold.lifecycles.get(rawKey);
  if (!existing) {
    fold.lifecycles.set(rawKey, {
      earliestTs: ts,
      sawNonDeleted: !deleted,
      knownSize: validSize,
      latest: {
        deleted,
        deletedAt,
        rowAt,
        version: validVersion,
      },
    });
    return;
  }

  if (ts !== null && (existing.earliestTs === null || ts < existing.earliestTs)) {
    existing.earliestTs = ts;
  }
  if (!deleted) {
    existing.sawNonDeleted = true;
  }
  if (existing.knownSize === undefined && validSize !== undefined) {
    existing.knownSize = validSize;
  }
  if (
    validVersion > existing.latest.version ||
    (validVersion === existing.latest.version && rowAt > existing.latest.rowAt)
  ) {
    existing.latest = {
      deleted,
      deletedAt,
      rowAt,
      version: validVersion,
    };
  }
}

export function foldIngestColumns(
  fold: IngestLifecycleFold,
  decoded: DecodedColumns,
  fromMs?: number,
  toMs?: number,
): void {
  const n = decoded.rows;
  fold.rowCount += n;
  if (n === 0) return;
  const cols = decoded.columns;
  const tsCol = cols.get("timestamp");
  const rawKeyCol = cols.get("raw_key");
  const rawSizeCol = cols.get("raw_size");
  const statusCol = cols.get("status");
  const versionCol = cols.get("state_version");
  const outputJsonCol = cols.get("output_keys_json");
  const deletedCol = cols.get("raw_deleted");
  const deletedAtCol = cols.get("raw_deleted_at");

  for (let i = 0; i < n; i++) {
    foldSingleRecord(
      fold,
      tsCol ? tsCol[i] : undefined,
      rawKeyCol ? rawKeyCol[i] : undefined,
      rawSizeCol ? rawSizeCol[i] : undefined,
      statusCol ? statusCol[i] : undefined,
      versionCol ? versionCol[i] : undefined,
      outputJsonCol ? outputJsonCol[i] : undefined,
      deletedCol ? deletedCol[i] : undefined,
      deletedAtCol ? deletedAtCol[i] : undefined,
      fromMs,
      toMs,
    );
  }
}

export function foldIngestRows(
  fold: IngestLifecycleFold,
  rows: readonly Record<string, unknown>[],
  fromMs?: number,
  toMs?: number,
): void {
  fold.rowCount += rows.length;
  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    foldSingleRecord(
      fold,
      row.timestamp,
      row.raw_key,
      row.raw_size,
      row.status,
      row.state_version,
      row.output_keys_json,
      row.raw_deleted,
      row.raw_deleted_at,
      fromMs,
      toMs,
    );
  }
}

export function mergeIngestLifecycleFold(
  target: IngestLifecycleFold,
  source: IngestLifecycleFold,
): void {
  target.rowCount += source.rowCount;
  if (source.minTs < target.minTs) target.minTs = source.minTs;
  if (source.maxTs > target.maxTs) target.maxTs = source.maxTs;
  for (const [key, output] of source.outputs) {
    const existing = target.outputs.get(key);
    if (!existing) {
      target.outputs.set(key, output);
    } else if (output.createdAt < existing.createdAt) {
      target.outputs.set(key, { ...existing, createdAt: output.createdAt });
    }
  }
  for (const key of source.deletedParquetKeys) {
    target.deletedParquetKeys.add(key);
  }
  for (const [rawKey, src] of source.lifecycles) {
    const existing = target.lifecycles.get(rawKey);
    if (!existing) {
      target.lifecycles.set(rawKey, {
        earliestTs: src.earliestTs,
        sawNonDeleted: src.sawNonDeleted,
        knownSize: src.knownSize,
        latest: { ...src.latest },
      });
      continue;
    }
    if (
      src.earliestTs !== null &&
      (existing.earliestTs === null || src.earliestTs < existing.earliestTs)
    ) {
      existing.earliestTs = src.earliestTs;
    }
    if (src.sawNonDeleted) {
      existing.sawNonDeleted = true;
    }
    if (existing.knownSize === undefined && src.knownSize !== undefined) {
      existing.knownSize = src.knownSize;
    }
    if (
      src.latest.version > existing.latest.version ||
      (src.latest.version === existing.latest.version &&
        src.latest.rowAt > existing.latest.rowAt)
    ) {
      existing.latest = { ...src.latest };
    }
  }
}

export function finalizeIngestLifecycle(
  fold: IngestLifecycleFold,
  options: {
    minimumBilledDays?: number;
    liveTruncated?: boolean;
    stats?: LogsStats;
  } = {},
): IngestLifecycleResult {
  const minimumBilledDays = options.minimumBilledDays ?? 30;
  const minWindowMs = minimumBilledDays * DAY_MS;
  const intervals: StorageBillingInterval[] = [];
  let bytes = 0;
  let complete = options.liveTruncated !== true;

  for (const [rawKey, entry] of fold.lifecycles) {
    if (!entry.latest.deleted) continue;
    const deletedAt = entry.latest.deletedAt ?? entry.latest.rowAt;
    if (entry.knownSize === undefined) {
      complete = false;
      continue;
    }
    const hasCreationEvidence =
      entry.sawNonDeleted ||
      (entry.earliestTs !== null && entry.earliestTs < deletedAt);
    const createdAt = entry.earliestTs ?? deletedAt;
    const minFloor = createdAt + minWindowMs;
    const billedUntil = Math.max(deletedAt, minFloor);
    const kind: StorageBillingIntervalKind =
      billedUntil > deletedAt ? "minimum_billed" : "deleted";
    const intervalComplete = hasCreationEvidence && options.liveTruncated !== true;
    if (!intervalComplete) {
      complete = false;
    }
    bytes += entry.knownSize;
    intervals.push({
      id: `raw_request:${rawKey}`,
      dataset: "raw_request",
      kind,
      bytes: entry.knownSize,
      createdAt: hasCreationEvidence ? createdAt : null,
      deletedAt,
      billedFrom: createdAt,
      billedUntil,
      complete: intervalComplete,
      provenance: "cloud:ingest_files",
    });
  }

  return {
    intervals,
    outputs: [...fold.outputs.values()],
    deletedParquetKeys: [...fold.deletedParquetKeys],
    bytes,
    complete,
    provenance: complete
      ? "cloud:ingest_files"
      : "cloud:ingest_files_incomplete",
    ...(options.liveTruncated !== undefined
      ? { liveTruncated: options.liveTruncated }
      : {}),
    ...(options.stats ? { stats: options.stats } : {}),
  };
}

type SerializedIngestFold = {
  v: 1;
  r: number;
  min: number | null;
  max: number | null;
  l: [string, number | null, 0 | 1, number | null, 0 | 1, number | null, number, number][];
  o: [string, number, number][];
  d: string[];
};

export function serializeIngestLifecycleFold(fold: IngestLifecycleFold): ArrayBuffer {
  const payload: SerializedIngestFold = {
    v: 1,
    r: fold.rowCount,
    min: Number.isFinite(fold.minTs) ? fold.minTs : null,
    max: Number.isFinite(fold.maxTs) ? fold.maxTs : null,
    l: [...fold.lifecycles].map(([key, state]) => [
      key,
      state.earliestTs,
      state.sawNonDeleted ? 1 : 0,
      state.knownSize ?? null,
      state.latest.deleted ? 1 : 0,
      state.latest.deletedAt,
      state.latest.rowAt,
      state.latest.version,
    ]),
    o: [...fold.outputs.values()].map((o) => [o.key, o.bytes, o.createdAt]),
    d: [...fold.deletedParquetKeys],
  };
  const bytes = new TextEncoder().encode(JSON.stringify(payload));
  return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
}

export function deserializeIngestLifecycleFold(bytes: ArrayBuffer): IngestLifecycleFold | null {
  let raw: SerializedIngestFold;
  try {
    raw = JSON.parse(new TextDecoder().decode(bytes)) as SerializedIngestFold;
  } catch {
    return null;
  }
  if (
    !raw ||
    raw.v !== 1 ||
    typeof raw.r !== "number" ||
    !Array.isArray(raw.l) ||
    !Array.isArray(raw.o) ||
    !Array.isArray(raw.d)
  ) {
    return null;
  }
  const fold = createIngestLifecycleFold();
  fold.rowCount = raw.r;
  fold.minTs = typeof raw.min === "number" ? raw.min : Infinity;
  fold.maxTs = typeof raw.max === "number" ? raw.max : -Infinity;
  for (let i = 0; i < raw.l.length; i++) {
    const [key, earliestTs, sawNonDeleted, knownSize, deleted, deletedAt, rowAt, version] =
      raw.l[i];
    fold.lifecycles.set(key, {
      earliestTs,
      sawNonDeleted: sawNonDeleted === 1,
      knownSize: knownSize === null ? undefined : knownSize,
      latest: {
        deleted: deleted === 1,
        deletedAt,
        rowAt,
        version,
      },
    });
  }
  for (let i = 0; i < raw.o.length; i++) {
    const [key, size, createdAt] = raw.o[i];
    fold.outputs.set(key, { key, bytes: size, createdAt });
  }
  for (let i = 0; i < raw.d.length; i++) {
    fold.deletedParquetKeys.add(raw.d[i]);
  }
  return fold;
}
