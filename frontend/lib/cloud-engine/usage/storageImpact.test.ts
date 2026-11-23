import { describe, expect, it } from "vitest";
import type { StorageBillingInterval } from "./storageBilling";
import { storageImpactBreakdown } from "./storageImpact";
import type { FosUsageRow } from "./summary";

const DAY = 86_400_000;
const now = 100 * DAY;
const window = { from: now - 30 * DAY, to: now };

const live = (dataset: string, bytes: number): StorageBillingInterval => ({
  id: `${dataset}:${bytes}`,
  dataset,
  kind: "live",
  bytes,
  billedFrom: now - DAY,
  billedUntil: now,
  complete: true,
  provenance: "cloud:inventory",
});

const deletion = (dataset: string, caller: string, bytes: number, at = now - DAY): FosUsageRow => ({
  timestamp: at,
  op: "DELETE_OBJECTS",
  op_class: "A",
  op_count: 1,
  dataset,
  caller,
  cdn_cache: "DIRECT",
  bytes_in: 0,
  bytes_out: 0,
  bytes_deleted: bytes,
  bytes_deleted_complete: true,
  est_cost_usd: 0,
});

describe("storageImpactBreakdown", () => {
  it("splits Cloud storage into the same rows High-Scale shows", () => {
    const out = storageImpactBreakdown({
      live: [live("pq_request", 700), live("pq_rum", 100), live("pq_ops", 50)],
      ingestDeleted: [{ ...live("raw_request", 400), kind: "minimum_billed", deletedAt: now - DAY }],
      fosRows: [
        deletion("raw_rum", "cron:raw_delete", 30),
        deletion("pq_request", "worker:request_compact", 200),
        deletion("pq_rum", "cron:rum_compact", 20),
        deletion("pq_request", "cron:retention", 90),
        deletion("pq_request", "cron:retention", 999, now - 40 * DAY),
        deletion("raw_request", "cron:raw_delete", 77),
      ],
      window,
    });
    expect(out).toEqual({
      requestLogBytes: 400,
      rumLogBytes: 30,
      processedRequestBytes: 700,
      processedRumBytes: 100,
      processedObjects: 2,
      compactionDeletedBytes: 220,
      retentionDeletedBytes: 90,
      rawLogsDeleted: true,
    });
  });

  it("counts raw request deletions from usage rows when exact ingest records are missing", () => {
    const out = storageImpactBreakdown({
      live: [],
      ingestDeleted: [],
      fosRows: [deletion("raw_request", "cron:raw_delete", 77)],
      window,
    });
    expect(out.requestLogBytes).toBe(77);
  });

  it("reports raw logs as kept when the service keeps them in the bucket", () => {
    const out = storageImpactBreakdown({
      live: [live("raw_request", 500), live("raw_rum", 40), live("pq_request", 10)],
      ingestDeleted: [],
      fosRows: [],
      window,
    });
    expect(out).toMatchObject({ rawLogsDeleted: false, requestLogBytes: 500, rumLogBytes: 40 });
  });
});
