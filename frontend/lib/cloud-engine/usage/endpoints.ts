import type { components } from "@/types/api.generated";
import type { MeasuredEstimate } from "./estimatePrefill";
import { LIST_RATES } from "./rates";
import { bucketUsage, type UsageBucketBy } from "./series";
import type { StorageBillingInterval, StorageBillingSummary } from "./storageBilling";
import { DEFAULT_SETTINGS, buildSevenLineCostTable, type ComputeBilling, type UsageInput, type UsageSummary } from "./summary";

type ApiSchemas = components["schemas"];

export type UsageCostTable = ReturnType<typeof buildSevenLineCostTable>;

export type CloudStorageIntervalWire = ApiSchemas["StorageBillingInterval"];

export type CloudCurrentStorageResponse = Omit<
  ApiSchemas["CurrentStorageResponse"],
  "storage_intervals"
> & {
  storage_intervals?: CloudStorageIntervalWire[];
  compute_requests?: number;
  compute_vcpu_ms?: number;
  compute_billed_vcpu_ms?: number;
  kv_ops?: number | null;
  cache_lookups?: number;
  cache_replaces?: number | null;
  purges?: number | null;
  incurred?: UsageCostTable;
  reconciliation?: UsageSummary["storage"];
  seven_line_table?: UsageCostTable["lines"];
};

export type CloudUsageOperationsPoint = ApiSchemas["UsageOperationsPoint"] & {
  kv_class_a?: number;
  kv_class_b?: number;
  kv_usd?: number;
};

export type CloudUsageOperationsResponse = Omit<ApiSchemas["UsageOperationsResponse"], "data"> & {
  data: CloudUsageOperationsPoint[];
};

export type CloudPrefillResponse = Omit<ApiSchemas["PrefillResponse"], "cloud_measured"> & {
  cloud_measured?: MeasuredEstimate | null;
};

export interface CloudStorageBillingInput {
  intervals: readonly StorageBillingInterval[];
  summary: StorageBillingSummary;
  observedAt: number | null;
  deletedBytes: number;
  deletedBytesComplete: boolean;
  deletedBytesProvenance: string;
  impact: {
    requestLogBytes: number;
    rumLogBytes: number;
    rawLogsDeleted: boolean;
    processedRequestBytes: number;
    processedRumBytes: number;
    processedObjects: number;
    compactionDeletedBytes: number;
    retentionDeletedBytes: number;
  };
  quarantineBytes?: number;
}

export interface BuildCloudUsageEndpointsArgs {
  fromMs: number;
  toMs: number;
  by: UsageBucketBy;
  input: UsageInput;
  summary: UsageSummary;
  storageBilling?: CloudStorageBillingInput | null;
  billing?: ComputeBilling | null;
  cdnBandwidthByHour?: Array<{ t: number; bytes: number }>;
  cdnRequestsByHour?: Array<{ t: number; requests: number }>;
  omitIntervals?: boolean;
  prefillEstimate?: MeasuredEstimate | null;
  estimatedBytesPerLine?: number | null;
}

function resolveBillingZone(region: string | null | undefined, egressRate: number): string {
  if (Math.abs(egressRate - 0.08) < 0.001) return "MBZ100";
  if (Math.abs(egressRate - 0.18) < 0.001) return "Global";
  if (Math.abs(egressRate - LIST_RATES.cdn_egress_rate_per_gb) < 0.001 && region) {
    const r = region.trim().toLowerCase();
    if (r.startsWith("us-") || r.startsWith("uk-") || r.startsWith("eu-")) return "MBZ100";
  }
  return "MBZ200";
}

function toSnakeCaseStorageInterval(interval: StorageBillingInterval): CloudStorageIntervalWire {
  return {
    id: interval.id,
    dataset: interval.dataset,
    kind: interval.kind,
    bytes: interval.bytes,
    created_at: interval.createdAt ?? null,
    deleted_at: interval.deletedAt ?? null,
    billed_from: interval.billedFrom,
    billed_until: interval.billedUntil,
    complete: interval.complete,
    provenance: interval.provenance,
  };
}

/**
 * Builds OpenAPI-aligned responses for all Cloud `/api/usage/*` endpoints so the
 * Cloud adapter and unit tests share one contract source of truth.
 */
export function buildCloudUsageEndpoints(args: BuildCloudUsageEndpointsArgs) {
  const {
    fromMs,
    toMs,
    by,
    input,
    summary,
    storageBilling = null,
    billing = null,
    cdnBandwidthByHour,
    cdnRequestsByHour,
    omitIntervals = false,
    prefillEstimate = null,
    estimatedBytesPerLine = null,
  } = args;

  const points = bucketUsage(
    input.rows,
    { from: fromMs, to: toMs, by },
    cdnBandwidthByHour,
    billing?.kvByHour,
    {
      classAPerMillion: input.settings.kv_class_a_rate_per_million,
      classBPerMillion: input.settings.kv_class_b_rate_per_million,
    },
    cdnRequestsByHour,
  );
  const startIso = new Date(fromMs).toISOString();
  const endIso = new Date(toMs).toISOString();
  const hasCdnRequests = Boolean(cdnRequestsByHour && cdnRequestsByHour.length > 0);

  return {
    currentStorage(): CloudCurrentStorageResponse {
      const sb = storageBilling;
      const intervals = sb?.intervals ?? [];
      const liveIntervals = intervals.filter((i) => i.kind === "live");
      const liveBytes = liveIntervals.length > 0
        ? liveIntervals.reduce((sum, i) => sum + i.bytes, 0)
        : summary.storage.totalBytes;
      const liveFiles = liveIntervals.length;
      const deletedBytes = sb?.deletedBytes ?? 0;
      const billedGbHours = sb?.summary.totalGbHours ?? 0;
      const archiveBytes = (sb?.impact.processedRequestBytes ?? 0) + (sb?.impact.processedRumBytes ?? 0);
      const archiveObjects = sb?.impact.processedObjects ?? 0;
      const observedAtMs = sb?.observedAt ?? summary.storage.snapshotAt;

      const costLines = buildSevenLineCostTable({
        hours: Math.max(0, (toMs - fromMs) / 3_600_000),
        settings: input.settings,
        billedGbHours,
        classA: summary.totals.classA,
        classB: summary.totals.classB,
        cdnHits: summary.totals.cdnHit,
        loggedCdnBytes: summary.bytesRead,
        loggedComputeRequests: summary.computeRequests,
        loggedComputeVcpuMs: summary.computeVcpuMs,
        loggedComputeSegments: summary.computeSegments,
        billing,
      });

      return {
        _is_cached: false,
        live_bytes: liveBytes,
        live_files: liveFiles,
        deleted_bytes: deletedBytes,
        deleted_bytes_complete: sb?.deletedBytesComplete ?? false,
        deleted_bytes_provenance: sb?.deletedBytesProvenance ?? "unknown",
        quarantine_bytes: sb?.quarantineBytes ?? 0,
        total_billed_bytes: liveBytes + deletedBytes,
        total_billed_gb_hours: billedGbHours,
        storage_billing_complete: sb?.summary.complete ?? false,
        storage_billing_provenance: sb?.summary.provenance ?? [],
        live_billed_gb_hours: sb?.summary.liveGbHours ?? 0,
        deleted_billed_gb_hours: (sb?.summary.deletedGbHours ?? 0) + (sb?.summary.minimumBilledGbHours ?? 0),
        minimum_billed_gb_hours: sb?.summary.minimumBilledGbHours ?? 0,
        ...(omitIntervals
          ? {}
          : { storage_intervals: intervals.map(toSnakeCaseStorageInterval) }),
        observed_at: observedAtMs !== null ? new Date(observedAtMs).toISOString() : null,
        archive_bytes: archiveBytes,
        archive_objects: archiveObjects,
        archive_expired_bytes: sb?.impact.retentionDeletedBytes ?? 0,
        cloud_copy_deleted_bytes: sb?.impact.compactionDeletedBytes ?? 0,
        total_files: liveFiles,
        total_bytes: liveBytes,
        start: startIso,
        end: endIso,
        rum_bytes: sb?.impact.rumLogBytes ?? 0,
        regular_log_bytes: sb?.impact.requestLogBytes ?? 0,
        processed_request_bytes: sb?.impact.processedRequestBytes ?? 0,
        processed_rum_bytes: sb?.impact.processedRumBytes ?? 0,
        processed_objects: archiveObjects,
        raw_logs_deleted: sb?.impact.rawLogsDeleted ?? true,
        compute_requests: summary.computeRequests,
        compute_vcpu_ms: summary.computeVcpuMs,
        compute_billed_vcpu_ms: summary.computeBilledVcpuMs ?? 0,
        kv_ops: summary.kvOps,
        cache_lookups: summary.cacheLookups,
        cache_replaces: summary.cacheReplaces,
        purges: summary.purges,
        incurred: costLines,
        reconciliation: summary.storage,
        seven_line_table: costLines.lines,
      };
    },

    operations(): CloudUsageOperationsResponse {
      const complete = input.telemetryCoverage?.fos_usage === true;
      return {
        _is_cached: false,
        source: "local",
        complete,
        provenance: ["fos_usage"],
        granularity: by,
        note: complete
          ? "Measured from this service's local Object Storage operation ledger."
          : "Local Object Storage operation ledger is incomplete or disabled for this range.",
        fos_fields_found: complete ? ["fos_usage.class_a", "fos_usage.class_b"] : [],
        total_class_a: summary.totals.classA,
        total_class_b: summary.totals.classB,
        data: points.map((p) => ({
          date: new Date(p.t).toISOString(),
          class_a: p.classA,
          class_b: p.classB,
          kv_class_a: p.kvClassA,
          kv_class_b: p.kvClassB,
          kv_usd: p.kvUsd,
        })),
      };
    },

    bandwidth(): ApiSchemas["UsageBandwidthResponse"] {
      const data = points.map((p) => ({
        time: new Date(p.t).toISOString(),
        bandwidth_bytes: p.bytesOut,
        requests: p.requests,
      }));
      return {
        _is_cached: false,
        total_bytes: data.reduce((sum, p) => sum + p.bandwidth_bytes, 0),
        total_log_bytes: summary.bytesRead,
        granularity: by,
        data,
      };
    },

    logActivity(): ApiSchemas["UsageLogActivityResponse"] {
      const data = points.map((p) => ({
        time: new Date(p.t).toISOString(),
        row_count: p.ingestedRows,
        bytes: p.bytesOut,
        api_requests: hasCdnRequests ? p.requests : null,
        fastly_log_records: null,
      }));
      const totalRows = data.reduce((sum, p) => sum + p.row_count, 0);
      const totalBytes = data.reduce((sum, p) => sum + p.bytes, 0);
      return {
        _is_cached: false,
        data,
        total_rows: totalRows,
        total_bytes: totalBytes,
        total_api_requests: hasCdnRequests ? points.reduce((sum, p) => sum + p.requests, 0) : null,
        total_fastly_log_records: null,
        granularity: by,
      };
    },

    prefill(): CloudPrefillResponse {
      return buildCloudPrefillResponse({
        prefillEstimate,
        settings: input.settings,
        dataDays: summary.days,
        estimatedBytesPerLine,
      });
    },

    prefillRates(): CloudPrefillRatesResponse {
      return buildCloudPrefillRatesResponse({
        settings: input.settings,
        prefillEstimate,
        dataDays: summary.days,
      });
    },
  };
}

export type CloudPrefillRatesResponse = ApiSchemas["PrefillRatesResponse"] & {
  compute_request_rate_per_million?: number;
};

export function buildCloudPrefillRatesResponse(args: {
  settings: UsageInput["settings"];
  prefillEstimate?: MeasuredEstimate | null;
  dataDays?: number;
}): CloudPrefillRatesResponse {
  const { settings, prefillEstimate = null, dataDays = 0 } = args;
  const inputs = prefillEstimate?.inputs;
  const zone = resolveBillingZone(prefillEstimate?.fosRegion, settings.cdn_egress_rate_per_gb);
  return {
    _is_cached: false,
    class_a_rate_per_1k: settings.class_a_rate_per_1k,
    class_b_rate_per_10k: settings.class_b_rate_per_10k,
    cdn_egress_rate_per_gb: settings.cdn_egress_rate_per_gb,
    storage_rate_per_gb_month: settings.storage_rate_per_gb_month,
    min_billed_days: settings.min_billed_days,
    compute_request_rate_per_million: settings.compute_request_rate_per_million,
    billing_zone: zone,
    fos_region: prefillEstimate?.fosRegion ?? null,
    data_days: prefillEstimate?.window.days ?? dataDays,
    log_period_seconds: inputs?.logPeriodS ?? null,
    sample_rate: inputs?.samplePct ?? 100,
    edge_only: inputs?.edgeOnly ?? false,
    delete_after: inputs?.deleteAfter ?? true,
    log_retention_days: inputs?.retentionDays ?? 30,
    rum_retention_days: inputs?.rumRetentionDays ?? 30,
    snapshot_interval_minutes: 5,
  };
}

export function buildCloudPrefillResponse(args: {
  prefillEstimate?: MeasuredEstimate | null;
  settings?: UsageInput["settings"];
  dataDays?: number;
  estimatedBytesPerLine?: number | null;
}): CloudPrefillResponse {
  const { prefillEstimate = null, settings = DEFAULT_SETTINGS, dataDays = 0, estimatedBytesPerLine = null } = args;
  const rates = prefillEstimate?.rates ?? settings;
  const inputs = prefillEstimate?.inputs;
  const zone = resolveBillingZone(prefillEstimate?.fosRegion, rates.cdn_egress_rate_per_gb);
  const fallbackBytes = inputs
    ? Math.max(1, Math.round(inputs.gzBytesPerLine * inputs.rawBytesPerGzByte))
    : null;
  return {
    _is_cached: false,
    class_a_rate_per_1k: rates.class_a_rate_per_1k,
    class_b_rate_per_10k: rates.class_b_rate_per_10k,
    cdn_egress_rate_per_gb: rates.cdn_egress_rate_per_gb,
    storage_rate_per_gb_month: rates.storage_rate_per_gb_month,
    min_billed_days: inputs?.minBilledDays ?? settings.min_billed_days,
    billing_zone: zone,
    fos_region: prefillEstimate?.fosRegion ?? null,
    estimated_bytes_per_line: estimatedBytesPerLine ?? fallbackBytes,
    data_days: prefillEstimate?.window.days ?? dataDays,
    log_period_seconds: inputs?.logPeriodS ?? null,
    sample_rate: inputs?.samplePct ?? 100,
    edge_only: inputs?.edgeOnly ?? false,
    delete_after: inputs?.deleteAfter ?? true,
    log_retention_days: inputs?.retentionDays ?? 30,
    rum_retention_days: inputs?.rumRetentionDays ?? 30,
    snapshot_interval_minutes: 5,
    requests_per_day: inputs ? Math.round(inputs.requestsPerDay) : null,
    edge_requests_per_day: inputs ? Math.round(inputs.edgeRequestsPerDay) : null,
    edge_ratio: inputs && inputs.requestsPerDay > 0
      ? Number(((inputs.edgeRequestsPerDay / inputs.requestsPerDay) * 100).toFixed(2))
      : null,
    rum_beacons_per_day: inputs ? Math.round(inputs.rumPageviewsPerDay) : null,
    ...(prefillEstimate ? { ...prefillEstimate, cloud_measured: prefillEstimate } : { rates }),
  };
}
