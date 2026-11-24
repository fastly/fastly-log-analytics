const BYTES_PER_GB = 1_000_000_000;
const MS_PER_HOUR = 3_600_000;

export type StorageBillingIntervalKind = "live" | "deleted" | "minimum_billed";

export type StorageBillingInterval = {
  id: string;
  dataset: string;
  kind: StorageBillingIntervalKind;
  bytes: number;
  createdAt?: number | null;
  deletedAt?: number | null;
  billedFrom: number;
  billedUntil: number;
  complete: boolean;
  provenance: string;
};

export type StorageBillingSummary = {
  liveGbHours: number;
  deletedGbHours: number;
  minimumBilledGbHours: number;
  totalGbHours: number;
  complete: boolean;
  provenance: string[];
};

export function calculateStorageBilling(
  intervals: readonly StorageBillingInterval[],
  from: number,
  to: number,
): StorageBillingSummary {
  if (!Number.isFinite(from) || !Number.isFinite(to) || to < from) {
    throw new RangeError(
      `Invalid storage billing range: expected finite timestamps with to >= from (got from=${from}, to=${to})`,
    );
  }

  if (to === from) {
    return {
      liveGbHours: 0,
      deletedGbHours: 0,
      minimumBilledGbHours: 0,
      totalGbHours: 0,
      complete: true,
      provenance: [],
    };
  }

  const seenIds = new Set<string>();
  const provenanceSet = new Set<string>();
  let liveGbHours = 0;
  let deletedGbHours = 0;
  let minimumBilledGbHours = 0;
  let complete = true;

  for (const interval of intervals) {
    if (seenIds.has(interval.id)) {
      continue;
    }
    seenIds.add(interval.id);

    if (!Number.isFinite(interval.billedFrom) || !Number.isFinite(interval.billedUntil)) {
      continue;
    }

    const overlapMs = Math.max(
      0,
      Math.min(interval.billedUntil, to) - Math.max(interval.billedFrom, from),
    );
    if (overlapMs <= 0) {
      continue;
    }

    const bytes = Number.isFinite(interval.bytes) && interval.bytes > 0 ? interval.bytes : 0;
    const gbHours = (bytes / BYTES_PER_GB) * (overlapMs / MS_PER_HOUR);

    switch (interval.kind) {
      case "live":
        liveGbHours += gbHours;
        break;
      case "deleted":
        deletedGbHours += gbHours;
        break;
      case "minimum_billed":
        minimumBilledGbHours += gbHours;
        break;
    }

    if (!interval.complete) {
      complete = false;
    }
    if (interval.provenance) {
      provenanceSet.add(interval.provenance);
    }
  }

  return {
    liveGbHours,
    deletedGbHours,
    minimumBilledGbHours,
    totalGbHours: liveGbHours + deletedGbHours + minimumBilledGbHours,
    complete,
    provenance: Array.from(provenanceSet).sort(),
  };
}
