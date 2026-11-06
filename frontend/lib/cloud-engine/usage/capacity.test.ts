import { describe, expect, it } from "vitest";
import { DEFAULT_INPUTS, type EstimateInputs } from "./estimate";
import { INGEST_LIMITS, ingestCapacity } from "./capacity";
import { INGEST_ASSUMPTIONS, planShards } from "./ingestShape";

const CPU = 113;

const atRps = (rps: number, overrides: Partial<EstimateInputs> = {}) => ({
  ...DEFAULT_INPUTS,
  requestsPerDay: rps * 86_400,
  edgeRequestsPerDay: rps * 86_400,
  gzBytesPerLine: 88,
  logPeriodS: 60,
  filesPerPeriod: 109,
  ...overrides,
});

describe("ingestCapacity", () => {
  it("fits the default small service well inside the direct envelope", () => {
    const shortPeriod = ingestCapacity(
      { ...DEFAULT_INPUTS, logPeriodS: 10 },
      {
        relay: 0,
        cpuMsPer1kLines: CPU,
      },
    );
    expect(shortPeriod.status).toBe("ok");
    expect(shortPeriod.averageRps).toBeCloseTo(1_000_000 / 86_400, 3);
    expect(shortPeriod.utilization).toBeLessThan(0.5);
    expect(shortPeriod.supportedRps).toBeGreaterThan(
      shortPeriod.averageRps * 5,
    );

    const defaults = ingestCapacity(DEFAULT_INPUTS, {
      relay: 0,
      cpuMsPer1kLines: CPU,
    });
    expect(defaults.status).toBe("cpu");
    expect(defaults.utilization).toBeLessThan(0.05);
    expect(defaults.throughputRps).toBeGreaterThan(defaults.averageRps * 5);
  });

  it("needs relayed fan-out for 100k requests a second of shard throughput", () => {
    const inputs = atRps(100_000);
    const direct = ingestCapacity(inputs, { relay: 0, cpuMsPer1kLines: CPU });
    const relayed = ingestCapacity(inputs, {
      relay: INGEST_LIMITS.maxRelayShards,
      cpuMsPer1kLines: CPU,
    });
    expect(direct.status).toBe("over");
    expect(direct.throughputRps).toBeLessThan(100_000);
    expect(relayed.status).not.toBe("over");
    expect(relayed.status).not.toBe("unsupported");
    expect(relayed.throughputRps).toBeGreaterThan(100_000);
    expect(relayed.shardsPerTick).toBe(
      Math.min(
        INGEST_LIMITS.workerCalls * INGEST_LIMITS.maxRelayShards,
        planShards(1),
      ),
    );
  });

  it("dispatches no more shards a tick than a plan within the KV read limit holds", () => {
    expect(planShards(1)).toBeGreaterThan(100);
    expect(planShards(1)).toBeLessThan(
      INGEST_LIMITS.workerCalls * INGEST_LIMITS.maxRelayShards,
    );
    expect(planShards(22)).toBeLessThan(planShards(1));
    const capacity = ingestCapacity(atRps(100_000, { logPeriodS: 300 }), {
      relay: INGEST_LIMITS.maxRelayShards,
      cpuMsPer1kLines: CPU,
    });
    expect(capacity.shardsPerTick).toBe(planShards(1));
  });

  it("puts the supported rate at the edge of the envelope", () => {
    const options = { relay: 4, cpuMsPer1kLines: CPU };
    const capacity = ingestCapacity(atRps(1_000), options);
    const edge = ingestCapacity(atRps(capacity.supportedRps), options);
    const past = ingestCapacity(atRps(capacity.supportedRps * 1.001), options);
    expect(edge.utilization).toBeLessThanOrEqual(1);
    expect(edge.status).not.toBe("unsupported");
    expect(edge.executionVcpuMs).toBeLessThanOrEqual(
      INGEST_LIMITS.executionVcpuMs,
    );
    expect(["over", "unsupported", "cpu"]).toContain(past.status);
  });

  it("does not call 100k a second supported past the documented 50 ms vCPU per execution", () => {
    const capacity = ingestCapacity(atRps(100_000, { logPeriodS: 300 }), {
      relay: INGEST_LIMITS.maxRelayShards,
      cpuMsPer1kLines: CPU,
    });
    const S = capacity.segmentsPerObject;
    expect(S).toBe(8);
    const skip =
      (S - 1) *
      (INGEST_LIMITS.shardDecompressedBytes / 1024 / 1024) *
      INGEST_ASSUMPTIONS.skipVcpuMsPerMib;
    expect(capacity.skipVcpuMs).toBeCloseTo(skip, 6);
    expect(capacity.executionVcpuMs).toBeGreaterThan(skip + 3000);
    expect(capacity.status).toBe("cpu");
    expect(capacity.reasons.join(" ")).toMatch(/50 ms/);
    expect(capacity.supportedRps).toBeLessThan(100_000);
    expect(capacity.throughputRps).toBeGreaterThan(capacity.supportedRps);
  });

  it("rejects raw objects too large to segment and points at the log period", () => {
    const inputs = atRps(200_000, { logPeriodS: 300, filesPerPeriod: 10 });
    const capacity = ingestCapacity(inputs, {
      relay: INGEST_LIMITS.maxRelayShards,
      cpuMsPer1kLines: CPU,
    });
    expect(capacity.objectGzBytes).toBeGreaterThan(INGEST_LIMITS.objectGzBytes);
    expect(capacity.status).toBe("unsupported");
    expect(capacity.reasons.join(" ")).toMatch(/log period/);
    const shorter = ingestCapacity(
      { ...inputs, logPeriodS: 10 },
      { relay: INGEST_LIMITS.maxRelayShards, cpuMsPer1kLines: CPU },
    );
    expect(shorter.status).not.toBe("unsupported");
  });

  it("segments objects past one shard's decompressed bytes", () => {
    const capacity = ingestCapacity(atRps(50_000, { filesPerPeriod: 20 }), {
      relay: 0,
      cpuMsPer1kLines: CPU,
    });
    expect(capacity.objectRawBytes).toBeGreaterThan(
      INGEST_LIMITS.shardDecompressedBytes,
    );
    expect(capacity.segmentsPerObject).toBe(
      Math.ceil(capacity.objectRawBytes / INGEST_LIMITS.shardDecompressedBytes),
    );
  });

  it("reports no load as idle", () => {
    const capacity = ingestCapacity(
      { ...DEFAULT_INPUTS, requestsPerDay: 0, edgeRequestsPerDay: 0 },
      { relay: 0, cpuMsPer1kLines: CPU },
    );
    expect(capacity.utilization).toBe(0);
    expect(capacity.status).toBe("ok");
  });
});
