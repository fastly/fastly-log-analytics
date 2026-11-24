import type { StorageBillingInterval } from "./storageBilling";
import type { FosUsageRow, IngestOutput } from "./summary";

/** The Storage Impact card's rows, read the way High-Scale reads them. */
export interface StorageImpactBreakdown {
  /** Raw request logs deleted after ingest inside the minimum-billing window (still billed). */
  requestLogBytes: number;
  /** Raw RUM logs deleted after ingest inside the minimum-billing window (still billed). */
  rumLogBytes: number;
  /** Live processed Parquet kept for retention, Cloud's counterpart of the High-Scale archive. */
  processedRequestBytes: number;
  processedRumBytes: number;
  processedObjects: number;
  /** Parquet compaction deleted inside the window. */
  compactionDeletedBytes: number;
  /** Parquet the retention job expired inside the window. */
  retentionDeletedBytes: number;
  /**
   * False when the service keeps raw logs (`delete_after` off): the raw files are then live objects and the
   * request and RUM log bytes above are what is kept, not deleted-but-billed.
   */
  rawLogsDeleted: boolean;
}

const PROCESSED = new Set(["pq_request", "pq_rum"]);

function isDeletion(row: FosUsageRow): boolean {
  return (row.op === "DELETE_OBJECTS" || row.op === "DELETE") && row.bytes_deleted > 0;
}

export function storageImpactBreakdown(input: {
  live: StorageBillingInterval[];
  /** Exact raw request deletions from `ingest_files`. */
  ingestDeleted: StorageBillingInterval[];
  fosRows: FosUsageRow[];
  window: { from: number; to: number };
}): StorageImpactBreakdown {
  const processed = input.live.filter((interval) => PROCESSED.has(interval.dataset));
  const deletions = input.fosRows.filter(
    (row) => isDeletion(row) && row.timestamp >= input.window.from && row.timestamp <= input.window.to,
  );
  const sum = (rows: FosUsageRow[]) => rows.reduce((total, row) => total + row.bytes_deleted, 0);
  const liveBytes = (dataset: string) =>
    input.live.filter((interval) => interval.dataset === dataset).reduce((total, interval) => total + interval.bytes, 0);
  const rawDeletions = deletions.filter((row) => row.dataset.startsWith("raw_"));
  // With deletion on, raw files are live only until ingest picks them up; kept raw outweighs what was deleted.
  const liveRaw = liveBytes("raw_request") + liveBytes("raw_rum");
  const rawLogsDeleted = input.ingestDeleted.length > 0 || rawDeletions.length > 0 || liveRaw === 0;
  const exactRequest = input.ingestDeleted
    .filter((interval) => interval.dataset === "raw_request")
    .reduce((total, interval) => total + interval.bytes, 0);
  return {
    requestLogBytes: !rawLogsDeleted
      ? liveBytes("raw_request")
      : input.ingestDeleted.length > 0
        ? exactRequest
        : sum(deletions.filter((row) => row.dataset === "raw_request")),
    rumLogBytes: rawLogsDeleted ? sum(deletions.filter((row) => row.dataset === "raw_rum")) : liveBytes("raw_rum"),
    processedRequestBytes: processed
      .filter((interval) => interval.dataset === "pq_request")
      .reduce((total, interval) => total + interval.bytes, 0),
    processedRumBytes: processed
      .filter((interval) => interval.dataset === "pq_rum")
      .reduce((total, interval) => total + interval.bytes, 0),
    processedObjects: processed.length,
    compactionDeletedBytes: sum(
      deletions.filter((row) => PROCESSED.has(row.dataset) && row.caller.includes("compact")),
    ),
    retentionDeletedBytes: sum(deletions.filter((row) => row.caller === "cron:retention")),
    rawLogsDeleted,
  };
}

/**
 * Live intervals for Parquet files ingest or compaction wrote after the inventory snapshot
 * completed and that the snapshot doesn't already hold (nor were subsequently deleted), so live
 * storage stays current between the 6-hourly snapshots.
 */
export function liveSinceSnapshot(
  outputs: IngestOutput[],
  snapshot: StorageBillingInterval[],
  observedAt: number | null,
  now: number,
  deletedKeys?: ReadonlySet<string>,
): StorageBillingInterval[] {
  const held = new Set(snapshot.map((interval) => interval.id));
  return outputs.flatMap((output) => {
    if (deletedKeys?.has(output.key)) return [];
    const dataset = output.key.includes("processed/rum/") ? "pq_rum" : "pq_request";
    const id = `${dataset}:${output.key}`;
    if (held.has(id) || (observedAt !== null && output.createdAt <= observedAt)) return [];
    return [{
      id,
      dataset,
      kind: "live" as const,
      bytes: output.bytes,
      createdAt: output.createdAt,
      deletedAt: null,
      billedFrom: output.createdAt,
      billedUntil: now,
      complete: true,
      provenance: "cloud:ingest_outputs",
    }];
  });
}
