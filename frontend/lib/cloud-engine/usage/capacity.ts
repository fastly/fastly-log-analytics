import {
  loggedLinesPerDay,
  rawFilesPerDay,
  type EstimateInputs,
} from "./estimate";
import {
  INGEST_ASSUMPTIONS,
  INGEST_LIMITS,
  ingestShape,
  ingestTick,
} from "./ingestShape";

export { INGEST_LIMITS, INGEST_TIMING } from "./ingestShape";

const MIB = 1024 * 1024;

export interface CapacityOptions {
  /** Config `ingest_relay`: shards per relayed worker call; 0 or 1 is direct dispatch. */
  relay: number;
  /** Worker vCPU per 1,000 lines, conversion plus summaries. */
  cpuMsPer1kLines: number;
}

export type CapacityStatus = "ok" | "near" | "over" | "cpu" | "unsupported";

export interface Capacity {
  /** Requests a second the logging condition sees, averaged over the day. */
  averageRps: number;
  linesPerSecond: number;
  filesPerSecond: number;
  objectGzBytes: number;
  objectRawBytes: number;
  segmentsPerObject: number;
  shardsPerTick: number;
  /** Worker wall time of a full shard. */
  shardSeconds: number;
  tickSeconds: number;
  /** Needed over supported shard (or listing) throughput. */
  utilization: number;
  /** vCPU of the busiest worker execution: its lines plus `skipVcpuMs`. */
  executionVcpuMs: number;
  /** vCPU the last segment spends decompressing the segments before its range. */
  skipVcpuMs: number;
  /** Highest average rate shard and listing throughput keep up with, ignoring vCPU. */
  throughputRps: number;
  /** Highest average rate that also keeps every execution within the documented vCPU. */
  supportedRps: number;
  status: CapacityStatus;
  reasons: string[];
}

const mb = (bytes: number) => `${(bytes / MIB).toFixed(1)} MiB`;

function requestsPerSecond(inputs: EstimateInputs): number {
  return (
    (inputs.edgeOnly ? inputs.edgeRequestsPerDay : inputs.requestsPerDay) /
    86_400
  );
}

function scaled(inputs: EstimateInputs, rps: number): EstimateInputs {
  const now = requestsPerSecond(inputs);
  if (now > 0) {
    const f = rps / now;
    return {
      ...inputs,
      requestsPerDay: inputs.requestsPerDay * f,
      edgeRequestsPerDay: inputs.edgeRequestsPerDay * f,
    };
  }
  return {
    ...inputs,
    requestsPerDay: rps * 86_400,
    edgeRequestsPerDay: rps * 86_400,
  };
}

function load(inputs: EstimateInputs, options: CapacityOptions) {
  const L = INGEST_LIMITS;
  const lps = loggedLinesPerDay(inputs) / 86_400;
  const fps = rawFilesPerDay(inputs) / 86_400;
  const shape = ingestShape(lps, fps, inputs.gzBytesPerLine);
  const { objectGz, objectRaw, segments } = shape;
  const { shardsPerSecond, shardsPerTick, shardSeconds, tickSeconds } =
    ingestTick(shape, fps, options.cpuMsPer1kLines, options.relay);
  const utilization =
    lps > 0
      ? Math.max(
          (shardsPerSecond * tickSeconds) / shardsPerTick,
          (fps * tickSeconds) / L.listPage,
        )
      : 0;
  const reasons: string[] = [];
  if (objectGz > L.objectGzBytes) {
    reasons.push(
      `Raw files average ${mb(objectGz)} compressed, past the ${mb(L.objectGzBytes)} one segment reads: shorten the log period.`,
    );
  } else if (objectRaw > L.objectDecompressedBytes) {
    reasons.push(
      `Raw files average ${mb(objectRaw)} decompressed, past the ${mb(L.objectDecompressedBytes)} that can be segmented: shorten the log period.`,
    );
  }
  const linesPerExecution = Math.min(
    shape.linesPerShard,
    lps * Math.max(inputs.logPeriodS, tickSeconds),
  );
  const skipVcpuMs =
    (segments - 1) *
    (L.shardDecompressedBytes / MIB) *
    INGEST_ASSUMPTIONS.skipVcpuMsPerMib;
  const executionVcpuMs =
    lps > 0
      ? (linesPerExecution * options.cpuMsPer1kLines) / 1000 + skipVcpuMs
      : 0;
  const cpu =
    executionVcpuMs > L.executionVcpuMs
      ? `The busiest worker execution needs about ${Math.round(executionVcpuMs)} ms vCPU${skipVcpuMs > 0 ? `, ${Math.round(skipVcpuMs)} ms of it skipping earlier segments` : ""}, past Fastly's documented ${L.executionVcpuMs} ms per execution. Ingest then relies on CPU beyond the documented limit.`
      : null;
  if (shardSeconds > L.workerStopS) {
    reasons.push(
      `A full shard needs about ${Math.round(shardSeconds)} s of worker time, past the ${L.workerStopS} s worker stop.`,
    );
  }
  return {
    lps,
    fps,
    objectGz,
    objectRaw,
    segments,
    shardsPerTick,
    shardSeconds,
    tickSeconds,
    utilization,
    executionVcpuMs,
    skipVcpuMs,
    cpu,
    reasons,
  };
}

function envelope(fits: (rps: number) => boolean): number {
  let lo = 0;
  let hi = 1e8;
  for (let i = 0; i < 80; i += 1) {
    const mid = (lo + hi) / 2;
    if (fits(mid)) lo = mid;
    else hi = mid;
  }
  return lo;
}

/**
 * Whether ingest keeps up with the service's average logged traffic: each tick plans at most
 * `LIST_PAGE` raw files into `workerCalls × relay` shards of at most 24 MiB decompressed, 22
 * files and 8 MiB compressed, and lasts the coordinator's fixed work plus one full shard.
 */
export function ingestCapacity(
  inputs: EstimateInputs,
  options: CapacityOptions,
): Capacity {
  const now = load(inputs, options);
  const at = (rps: number) => load(scaled(inputs, rps), options);
  const keepsUp = (l: ReturnType<typeof load>) =>
    l.reasons.length === 0 && l.utilization <= 1;
  const throughputRps = envelope((rps) => keepsUp(at(rps)));
  const supportedRps = envelope((rps) => {
    const l = at(rps);
    return keepsUp(l) && l.cpu === null;
  });
  const status: CapacityStatus =
    now.reasons.length > 0
      ? "unsupported"
      : now.utilization > 1
        ? "over"
        : now.cpu !== null
          ? "cpu"
          : now.utilization > 0.8
            ? "near"
            : "ok";
  return {
    averageRps: requestsPerSecond(inputs),
    linesPerSecond: now.lps,
    filesPerSecond: now.fps,
    objectGzBytes: now.objectGz,
    objectRawBytes: now.objectRaw,
    segmentsPerObject: now.segments,
    shardsPerTick: now.shardsPerTick,
    shardSeconds: now.shardSeconds,
    tickSeconds: now.tickSeconds,
    utilization: now.utilization,
    executionVcpuMs: now.executionVcpuMs,
    skipVcpuMs: now.skipVcpuMs,
    throughputRps,
    supportedRps,
    status,
    reasons: now.cpu === null ? now.reasons : [...now.reasons, now.cpu],
  };
}

export const MAX_TICK_BACKEND_CALLS = 32;
export const MAX_TICK_VCPU_MS = 1000;
export const evaluateCapacity = ingestCapacity;
