const MIB = 1024 * 1024;

/** The ingest pipeline's limits, mirrored from `app/src/ingest`. */
export const INGEST_LIMITS = {
  /** Worker calls per tick: `coordinator.rs` `DEFAULT_FANOUT` / `MAX_FANOUT`. */
  workerCalls: 8,
  /** Shards one relay call carries: `worker.rs` `MAX_RELAY_SHARDS`. */
  maxRelayShards: 16,
  /** `worker.rs` `SHARD_MAX_OBJECTS`. */
  shardObjects: 22,
  /** `coordinator.rs` `SHARD_MAX_GZ_BYTES`. */
  shardGzBytes: 8 * MIB,
  /** `mod.rs` `MAX_BATCH_DECOMPRESSED_BYTES` and the default segment size (`ingest_segment_kib`). */
  shardDecompressedBytes: 24 * MIB,
  /** Largest compressed object a segment reads: `mod.rs` `SEGMENT_READ_LIMIT`. */
  objectGzBytes: 24 * MIB,
  /** Largest object that can be measured and segmented: `convert.rs` `MAX_OBJECT_DECOMPRESSED_BYTES`. */
  objectDecompressedBytes: 512 * MIB,
  /** The coordinator's compression-ratio estimate: `coordinator.rs` `ESTIMATED_RATIO`. */
  rawBytesPerGzByte: 8,
  /** Raw keys listed per plan: `mod.rs` `LIST_PAGE`. */
  listPage: 200,
  /** `worker.rs` `WORKER_STOP`. */
  workerStopS: 28,
  /** Fastly's documented vCPU per Compute execution (compute-resource-limits). */
  executionVcpuMs: 50,
  /** Multipart part size; larger window files upload in parts: `compaction.rs` `PART_BYTES`. */
  partBytes: 8 * MIB,
  /** Largest saved plan, the SDK's KV read limit: `coordinator.rs` `PLAN_BYTES`. */
  planBytes: 2_097_152,
} as const;

/**
 * JSON the coordinator reserves in a plan for each pending shard's result, mirrored from
 * `coordinator.rs` `result_reserve` / `pending_bytes`. Key and prefix lengths are typical values.
 */
export const PLAN_RESERVE = {
  /** `worker.rs` `WORKER_CALLS` less `WORKER_SECRET_READS`, the manifest PUT and one spare. */
  windowCalls: 32 - 3 - 2,
  /** `mod.rs` `MAX_WINDOWS_PER_OBJECT`. */
  objectWindows: 12,
  outputJsonBytes: 320,
  objectJsonBytes: 256,
  resultJsonBytes: 320,
  /** A bad-line sample: `worker.rs` `SAMPLE_BYTES` (512), every byte JSON-escaped. */
  sampleJsonBytes: 6 * 512 + 2,
  planSlackBytes: 4096,
  segmentJsonBytes: 64,
  rawKeyBytes: 130,
  prefixBytes: 32,
  tableBytes: 7,
} as const;

/** Shards of `objects` raw files each that one plan can hold within `planBytes`. */
export function planShards(objects: number): number {
  const R = PLAN_RESERVE;
  const k = Math.max(1, objects);
  const windows = Math.min(
    Math.floor((R.windowCalls - k) / 2),
    R.objectWindows * k,
  );
  const output = R.prefixBytes + R.tableBytes + R.outputJsonBytes;
  const perObject =
    R.rawKeyBytes +
    R.objectJsonBytes +
    R.sampleJsonBytes +
    Math.min(windows, R.objectWindows) * output;
  const entry = k * (R.rawKeyBytes + 1) + R.segmentJsonBytes + 8;
  const shard =
    R.resultJsonBytes +
    R.prefixBytes +
    2 * windows * output +
    k * perObject +
    entry;
  return Math.max(
    1,
    Math.floor((INGEST_LIMITS.planBytes - R.planSlackBytes) / shard),
  );
}

/** Planning times from the live research (docs/research/07-live-verification.md §6). */
export const INGEST_TIMING = {
  /** Coordinator calls before and after the dispatch, plus the 1 s clock gap. */
  tickFixedS: 5,
  /** One raw object GET (measured GET wait ≤ 133 ms). */
  getS: 0.15,
  /** One window file and its summary PUT. */
  windowS: 0.5,
  windowsPerShard: 2,
} as const;

/** Unmeasured costs of the high-RPS paths, to be replaced by Task 25.4 measurements. */
export const INGEST_ASSUMPTIONS = {
  /** Worker vCPU to decompress and skip 1 MiB before a segment's range (~100 MiB/s inflate). */
  skipVcpuMsPerMib: 10,
  /** One relay execution: parse its body, dispatch its shards and merge their replies. */
  relayVcpuMsPerCall: 5,
} as const;

export interface IngestShape {
  objectGz: number;
  objectRaw: number;
  /** Shards one raw object is cut into; 1 when it converts whole. */
  segments: number;
  objectsPerShard: number;
  linesPerShard: number;
}

/**
 * The average raw object and how the coordinator packs it, following `coordinator.rs`
 * `segment_count`: an object whose estimated decompressed size exceeds two segments, and that
 * a segment can still read, becomes `ceil(raw / segment)` one-object shards.
 */
export function ingestShape(
  linesPerDay: number,
  filesPerDay: number,
  gzBytesPerLine: number,
): IngestShape {
  const L = INGEST_LIMITS;
  const lineGz = Math.max(1, gzBytesPerLine);
  const lineRaw = lineGz * L.rawBytesPerGzByte;
  const objectGz = filesPerDay > 0 ? (linesPerDay * lineGz) / filesPerDay : 0;
  const objectRaw = objectGz * L.rawBytesPerGzByte;
  const segments =
    objectGz <= L.objectGzBytes && objectRaw > 2 * L.shardDecompressedBytes
      ? Math.ceil(objectRaw / L.shardDecompressedBytes)
      : 1;
  const objectsPerShard =
    segments > 1
      ? 1
      : objectRaw <= 0
        ? L.shardObjects
        : Math.max(
            1,
            Math.min(
              L.shardObjects,
              Math.floor(L.shardDecompressedBytes / objectRaw),
              Math.floor(L.shardGzBytes / objectGz),
            ),
          );
  const linesPerShard =
    segments > 1
      ? L.shardDecompressedBytes / lineRaw
      : (objectsPerShard * objectRaw) / lineRaw;
  return { objectGz, objectRaw, segments, objectsPerShard, linesPerShard };
}

export interface IngestTick {
  shardsPerSecond: number;
  /** Shards one tick can dispatch: `workerCalls × max(relay, 1)`, at most `planShards`. */
  shardsPerTick: number;
  shardSeconds: number;
  tickSeconds: number;
  /** Relay calls one tick makes for the shards it has pending; 0 when dispatching directly. */
  relayCallsPerTick: number;
}

export const relayShards = (relay: number) =>
  Math.min(INGEST_LIMITS.maxRelayShards, Math.max(1, Math.floor(relay)));

/** A tick's coordinator work plus one full shard, at `cpuMsPer1kLines` worker vCPU. */
export function ingestTick(
  shape: IngestShape,
  filesPerSecond: number,
  cpuMsPer1kLines: number,
  relay: number,
): IngestTick {
  const shardSeconds =
    shape.objectsPerShard * INGEST_TIMING.getS +
    (shape.linesPerShard * cpuMsPer1kLines) / 1e6 +
    INGEST_TIMING.windowsPerShard * INGEST_TIMING.windowS;
  const tickSeconds = INGEST_TIMING.tickFixedS + shardSeconds;
  const r = relayShards(relay);
  const shardsPerTick = Math.min(
    INGEST_LIMITS.workerCalls * r,
    planShards(shape.objectsPerShard),
  );
  const shardsPerSecond =
    (filesPerSecond * shape.segments) / shape.objectsPerShard;
  const pending = Math.min(shardsPerSecond * tickSeconds, shardsPerTick);
  return {
    shardsPerSecond,
    shardsPerTick,
    shardSeconds,
    tickSeconds,
    relayCallsPerTick: r > 1 && pending >= 2 ? Math.ceil(pending / r) : 0,
  };
}

export interface IngestFileSummary {
  fileCount: number;
  totalRows: number;
  totalRawBytes: number;
  totalGzBytes: number;
  totalParquetBytes: number;
  avgGzBytesPerRow: number;
  avgRawBytesPerRow: number;
}

/** Summarizes an array of ingest_files rows safely (0-safe when empty). */
export function summarizeIngestFiles(
  rows: ReadonlyArray<{
    rows?: number;
    raw_bytes?: number;
    gz_bytes?: number;
    parquet_bytes?: number;
  }>,
): IngestFileSummary {
  let totalRows = 0;
  let totalRawBytes = 0;
  let totalGzBytes = 0;
  let totalParquetBytes = 0;
  for (const r of rows) {
    totalRows += Math.max(0, Number(r.rows ?? 0) || 0);
    totalRawBytes += Math.max(0, Number(r.raw_bytes ?? 0) || 0);
    totalGzBytes += Math.max(0, Number(r.gz_bytes ?? 0) || 0);
    totalParquetBytes += Math.max(0, Number(r.parquet_bytes ?? 0) || 0);
  }
  return {
    fileCount: rows.length,
    totalRows,
    totalRawBytes,
    totalGzBytes,
    totalParquetBytes,
    avgGzBytesPerRow: totalRows > 0 ? totalGzBytes / totalRows : 0,
    avgRawBytesPerRow: totalRows > 0 ? totalRawBytes / totalRows : 0,
  };
}
