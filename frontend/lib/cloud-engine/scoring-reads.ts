import { cloudActiveLogFieldIds } from "./active-log-fields";
import { CloudBudgetError, cloudReadContext } from "./range-loader";
import { computeRocAuc } from "./scoring-train";
import { roundTo } from "./engine-utils";
import { buildScoringSourceSql } from "./sessions";
import { duckSqlString } from "./dashboard/duckdb";
import { useSessionRoleStore } from "@/stores/filterStore";
import { maskIp } from "./pii-mask";

export const CLOUD_SCORING_CAPABILITIES = {
  historical_analytics: true,
  management: true,
  labels: true,
  evaluation: true,
  thresholds: true,
  matrix_versions: true,
  audit: true,
} as const;

export const CLOUD_SCORING_SCAN_ROUTES = new Set([
  "analytics", "top-flagged", "score-distribution", "compliance-breakdown",
  "latency-timeseries", "health", "evaluation", "evaluation/per-reason", "curves", "threshold-preview",
]);

/** Narrow scoring columns read in pass 1 for overview/aggregate/evaluation scoring routes.
 * Excludes wide request strings (url, ua, ip, status, country) so range-loader uses HTTP range
 * reads instead of falling back to whole-file Parquet downloads. */
export const SCORING_CORE_COLUMNS = [
  "timestamp", "ts_ms", "edge", "edge_sid", "edge_score", "edge_score_l1", "edge_score_l2",
  "edge_score_reason", "edge_cookie_compliance", "edge_score_rtt_us", "edge_score_exec_us",
] as const;

/** Wide request detail columns fetched in a bounded pass-2 range load only for top-flagged rows. */
export const SCORING_ENRICH_COLUMNS = [
  "timestamp", "ts_ms", "edge_sid", "edge_score", "ip", "ua", "url", "status", "country",
] as const;

/** Columns read by the single-session event drill-down (`sessions/{sid}/events`). */
export const SCORING_EVENTS_COLUMNS = [
  "timestamp", "ts_ms", "edge_sid", "edge_score", "edge_score_l1", "edge_score_l2",
  "edge_score_reason", "edge_cookie_compliance", "url", "status",
] as const;

export const SCORING_SCAN_COLUMNS = SCORING_CORE_COLUMNS;

export type ScoringRangeLoader = (
  fromMs: number,
  toMs: number,
  columns: readonly string[] | null,
) => Promise<{
  runSql: (sql: string) => Promise<Record<string, unknown>[]>;
  release?: () => void;
  presentColumns?: () => Promise<Set<string>>;
}>;

export interface ScoringReadOptions {
  loadRange?: ScoringRangeLoader;
}

export function scoringScanColumns(route?: string): string[] {
  if (route && /^sessions\/[^/]+\/events$/.test(route)) {
    return [...SCORING_EVENTS_COLUMNS];
  }
  return [...SCORING_CORE_COLUMNS];
}

export function scoringRequiredAnyColumns(route?: string): string[] {
  if (!route) return [];
  if (/^sessions\/[^/]+\/events$/.test(route)) {
    return ["edge_score", "edge_sid"];
  }
  if (CLOUD_SCORING_SCAN_ROUTES.has(route)) {
    return ["edge_score"];
  }
  return [];
}

export const SCORING_MIN_LABELS_PER_CLASS = 3;
export const SCORING_KNOWN_REASON_ATOMS = [
  "cookie-missing",
  "cookie-tampered",
  "cookie-expired",
  "cookie-replayed",
  "impossibly-fast",
  "robotic-consistency",
  "low-transition-prob",
] as const;

export class CloudScoringHttpError extends Error {
  readonly status: number;
  readonly body: unknown;

  constructor(status: number, body: unknown, message = `Scoring HTTP ${status}`) {
    super(message);
    this.name = "CloudScoringHttpError";
    this.status = status;
    this.body = body;
  }
}

export interface ScoringUnavailablePayload {
  supported: boolean;
  available: false;
  reason: string;
  rows?: unknown[];
  since_hours?: number;
  since_days?: number;
  has_latency?: boolean;
  granularity?: string;
  total_edge_rows?: number;
  scored_rows?: number;
  fire_rate_pct?: number;
  distinct_sids?: number;
  avg_score?: null;
  p50_score?: null;
  p95_score?: null;
  max_score?: null;
  scorer_errors?: number;
  fail_open_rate_pct?: number;
  top_reasons?: unknown[];
  fail_open_breakdown?: unknown[];
  latency?: { available: false };
  matrix_staleness?: null;
  l2_enforce?: null;
  threshold?: number;
  total_scored_sessions?: number;
  flagged?: { total: number; good: number; bad: number; unlabeled: number };
  passed?: boolean | { good: number; bad: number; unlabeled: number };
  precision?: null;
  recall?: null;
  n_good?: number;
  n_bad?: number;
  min_per_class?: number;
  has_min_samples?: boolean;
  auc?: number | null;
  default_min_auc?: number;
  buckets?: unknown[];
  known_reasons?: string[];
  has_min_samples_overall?: boolean;
  average_precision?: number | null;
  roc?: unknown[];
  pr?: unknown[];
  sid?: string;
  event_count?: number;
  events?: unknown[];
  capabilities?: typeof CLOUD_SCORING_CAPABILITIES;
  top_flagged?: { rows: unknown[]; since_hours: number };
  score_distribution?: { rows: unknown[]; since_hours: number };
  compliance_breakdown?: ScoringUnavailablePayload;
  latency_timeseries?: ScoringUnavailablePayload;
  health?: ScoringUnavailablePayload;
  evaluation?: ReturnType<typeof emptyEvaluation>["evaluation"];
  evaluation_per_reason?: ReturnType<typeof emptyEvaluation>["evaluation_per_reason"];
}

function unavailableBase(reason: string, supported = false) {
  return { supported, available: false as const, reason };
}

function unavailableForRoute(
  route: string,
  reason: string,
  opts: {
    supported?: boolean;
    sinceHours?: number;
    sinceDays?: number;
    threshold?: number;
    sid?: string;
  } = {},
): ScoringUnavailablePayload {
  const supported = opts.supported ?? false;
  const sinceHours = opts.sinceHours ?? 24;
  const sinceDays = opts.sinceDays ?? 30;
  const threshold = opts.threshold ?? 75;
  const base = unavailableBase(reason, supported);

  if (route === "top-flagged" || route === "score-distribution" || route === "compliance-breakdown") {
    return { ...base, rows: [] as unknown[], since_hours: sinceHours };
  }
  if (route === "latency-timeseries") {
    return {
      ...base,
      rows: [] as unknown[],
      since_hours: sinceHours,
      has_latency: false,
      granularity: sinceHours <= 1 ? "minute" : "hour",
    };
  }
  if (route === "health") {
    return {
      ...base,
      since_hours: sinceHours,
      total_edge_rows: 0,
      scored_rows: 0,
      fire_rate_pct: 0,
      distinct_sids: 0,
      avg_score: null,
      p50_score: null,
      p95_score: null,
      max_score: null,
      scorer_errors: 0,
      fail_open_rate_pct: 0,
      top_reasons: [] as unknown[],
      fail_open_breakdown: [] as unknown[],
      latency: { available: false },
      matrix_staleness: null,
      l2_enforce: null,
    };
  }
  if (route === "threshold-preview") {
    return {
      ...base,
      since_hours: sinceHours,
      threshold,
      total_scored_sessions: 0,
      flagged: { total: 0, good: 0, bad: 0, unlabeled: 0 },
      passed: { good: 0, bad: 0, unlabeled: 0 },
      precision: null,
      recall: null,
    };
  }
  if (route === "evaluation" || route === "evaluation/per-reason" || route === "curves") {
    const empty = emptyEvaluation(0, 0, false, null);
    if (route === "evaluation/per-reason") return { ...base, ...empty.evaluation_per_reason };
    if (route === "curves") return { ...base, ...empty.curves };
    return { ...base, ...empty.evaluation };
  }
  if (route.startsWith("sessions/")) {
    return { ...base, sid: opts.sid ?? "", since_days: sinceDays, event_count: 0, events: [] as unknown[] };
  }
  const empty = emptyEvaluation(0, 0, false, null);
  return {
    ...base,
    since_hours: sinceHours,
    capabilities: CLOUD_SCORING_CAPABILITIES,
    top_flagged: { rows: [] as unknown[], since_hours: sinceHours },
    score_distribution: { rows: [] as unknown[], since_hours: sinceHours },
    compliance_breakdown: { ...base, rows: [] as unknown[], since_hours: sinceHours },
    latency_timeseries: {
      ...base,
      rows: [] as unknown[],
      since_hours: sinceHours,
      has_latency: false,
      granularity: sinceHours <= 1 ? "minute" : "hour",
    },
    health: unavailableForRoute("health", reason, { supported, sinceHours }),
    evaluation: empty.evaluation,
    evaluation_per_reason: empty.evaluation_per_reason,
  };
}

function bounded(value: string | null, fallback: number, max: number, min = 1): number {
  if (value == null || value === "") return fallback;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Math.min(max, Math.max(min, Math.floor(parsed))) : fallback;
}

type ScoringLabel = { sid?: string; edge_sid?: string; label: string };

function labelSid(label: ScoringLabel): string {
  return label.sid ?? label.edge_sid ?? "";
}

function decodeScoringServiceId(pathname: string): string {
  const match = pathname.match(/^\/api\/services\/([^/]+)\/scoring\//);
  if (!match) return "";
  try {
    return decodeURIComponent(match[1]);
  } catch {
    return "";
  }
}

async function fetchScoringLabels(serviceId: string, signal?: AbortSignal): Promise<ScoringLabel[]> {
  if (!serviceId) return [];
  try {
    const resp = await fetch(`/api/services/${encodeURIComponent(serviceId)}/scoring/labels`, { signal });
    if (!resp.ok) return [];
    const data = (await resp.json()) as { labels?: ScoringLabel[] };
    return Array.isArray(data.labels) ? data.labels : [];
  } catch (error) {
    if (signal?.aborted) throw error;
    return [];
  }
}

function trapezoidArea(points: Array<[number, number]>): number {
  if (points.length < 2) return 0;
  const sorted = [...points].sort((a, b) => a[0] - b[0]);
  let area = 0;
  for (let i = 1; i < sorted.length; i++) {
    area += (sorted[i][0] - sorted[i - 1][0]) * (sorted[i - 1][1] + sorted[i][1]) / 2;
  }
  return area;
}

function emptyEvaluation(nGood: number, nBad: number, hasMinSamples: boolean, auc: number | null = null) {
  return {
    evaluation: {
      n_good: nGood,
      n_bad: nBad,
      min_per_class: SCORING_MIN_LABELS_PER_CLASS,
      has_min_samples: hasMinSamples,
      auc,
      threshold: 0.85,
      default_min_auc: 0.85,
      passed: auc !== null && auc >= 0.85,
    },
    evaluation_per_reason: {
      buckets: [] as unknown[],
      n_good: nGood,
      n_bad: nBad,
      min_per_class: SCORING_MIN_LABELS_PER_CLASS,
      known_reasons: [...SCORING_KNOWN_REASON_ATOMS],
      has_min_samples_overall: hasMinSamples,
    },
    curves: {
      has_min_samples: hasMinSamples && auc !== null,
      min_per_class: SCORING_MIN_LABELS_PER_CLASS,
      n_good: nGood,
      n_bad: nBad,
      auc,
      average_precision: null as number | null,
      roc: [] as unknown[],
      pr: [] as unknown[],
    },
  };
}

/** Short-circuit scoring routes before any Parquet scan when edge_score is disabled or
 * evaluation/curves/per-reason do not need raw log data. */
export async function cloudScoringPreflight(request: Request): Promise<Response | null> {
  const url = new URL(request.url);
  const match = url.pathname.match(/^\/api\/services\/([^/]+)\/scoring\/(.+)$/);
  if (!match || request.method !== "GET") return null;
  const [, , route] = match;
  if (!CLOUD_SCORING_SCAN_ROUTES.has(route) && !/^sessions\/[^/]+\/events$/.test(route)) return null;
  const serviceId = decodeScoringServiceId(url.pathname);
  const sinceHours = bounded(url.searchParams.get("since_hours"), 24, 168);
  const sinceDays = bounded(url.searchParams.get("since_days"), 30, 90);
  const threshold = bounded(url.searchParams.get("threshold"), 75, 100, 0);
  if (!(await cloudActiveLogFieldIds(serviceId || undefined, request.signal)).has("edge_score")) {
    return Response.json(
      unavailableForRoute(route, "scoring_fields_not_present", {
        supported: false,
        sinceHours,
        sinceDays,
        threshold,
      }),
    );
  }
  if (route === "evaluation" || route === "evaluation/per-reason" || route === "curves") {
    const labels = await fetchScoringLabels(serviceId, request.signal);
    const nGood = labels.filter((l) => l.label === "good").length;
    const nBad = labels.filter((l) => l.label === "bad").length;
    const hasMinSamples =
      nGood >= SCORING_MIN_LABELS_PER_CLASS && nBad >= SCORING_MIN_LABELS_PER_CLASS;
    if (!hasMinSamples) {
      const empty = emptyEvaluation(nGood, nBad, false);
      if (route === "evaluation/per-reason") return Response.json(empty.evaluation_per_reason);
      return Response.json(route === "curves" ? empty.curves : empty.evaluation);
    }
  }
  return null;
}

function rowEnrichKey(tsMs: unknown, sid: unknown, score: unknown): string {
  return `${Number(tsMs)}|${sid == null ? "" : String(sid)}|${Number(score)}`;
}

async function enrichTopFlaggedRows(
  baseRows: Record<string, unknown>[],
  loadRange: ScoringRangeLoader,
  maskIps: boolean,
): Promise<Record<string, unknown>[]> {
  if (baseRows.length === 0) return [];
  const stripTsMs = (row: Record<string, unknown>, detail?: Record<string, unknown>) => {
    const rest = { ...row };
    delete rest.ts_ms;
    return {
      ...rest,
      ip: maskIps ? (detail?.ip != null ? maskIp(String(detail.ip)) : "[redacted]") : (detail?.ip ?? null),
      ua: detail?.ua ?? null,
      url: detail?.url ?? null,
      status: detail?.status ?? null,
      country: detail?.country ?? null,
    };
  };
  const timestamps = baseRows.map((row) => Number(row.ts_ms)).filter((ms) => Number.isFinite(ms));
  if (timestamps.length === 0) return baseRows.map((row) => stripTsMs(row));
  let minTs = timestamps[0];
  let maxTs = timestamps[0];
  for (const ts of timestamps) {
    if (ts < minTs) minTs = ts;
    if (ts > maxTs) maxTs = ts;
  }
  const enrichCols = maskIps
    ? SCORING_ENRICH_COLUMNS.filter((c) => c !== "ip")
    : [...SCORING_ENRICH_COLUMNS];
  let loaded: Awaited<ReturnType<ScoringRangeLoader>> | undefined;
  try {
    loaded = await loadRange(minTs, maxTs + 2, enrichCols);
  } catch (error) {
    if (error instanceof CloudBudgetError) {
      return baseRows.map((row) => stripTsMs(row));
    }
    throw error;
  }
  try {
    const present = loaded.presentColumns ? await loaded.presentColumns() : new Set<string>(enrichCols);
    const enrichSource = buildScoringSourceSql("requests", present);
    const distinctTs = [...new Set(timestamps)];
    const detailRows = await loaded.runSql(`SELECT ts_ms, edge_sid, edge_score,
      ${maskIps ? "CAST(NULL AS VARCHAR) AS ip" : "ip"}, ua, url, status, country FROM ${enrichSource}
      WHERE ts_ms >= ${minTs} AND ts_ms <= ${maxTs}
        AND ts_ms IN (${distinctTs.join(", ")})
        AND edge_score IS NOT NULL`);
    const byKey = new Map<string, Record<string, unknown>[]>();
    for (const detail of detailRows) {
      const key = rowEnrichKey(detail.ts_ms, detail.edge_sid, detail.edge_score);
      const list = byKey.get(key);
      if (list) list.push(detail);
      else byKey.set(key, [detail]);
    }
    return baseRows.map((row) => {
      const key = rowEnrichKey(row.ts_ms, row.edge_sid, row.edge_score);
      const detail = byKey.get(key)?.shift();
      return stripTsMs(row, detail);
    });
  } finally {
    loaded.release?.();
  }
}

export async function queryCloudScoring(
  request: Request,
  route: string,
  options?: ScoringReadOptions,
) {
  const url = new URL(request.url);
  const serviceId = decodeScoringServiceId(url.pathname) || undefined;
  const maskIps = request.headers.get("x-mask-ips") === "1" ||
    request.headers.get("x-session-role") === "analyst-no-pii" ||
    ["1", "true"].includes(url.searchParams.get("mask_ips") ?? "") ||
    useSessionRoleStore.getState().isRemoteAnalyst;
  const sinceHours = bounded(url.searchParams.get("since_hours"), 24, 168);
  const sinceDays = bounded(url.searchParams.get("since_days"), 30, 90);
  const previewThreshold = bounded(url.searchParams.get("threshold"), 75, 100, 0);
  const eventMatch = route.match(/^sessions\/([^/]+)\/events$/);
  if (eventMatch && maskIps) {
    throw new CloudScoringHttpError(403, {
      detail: {
        error: "pii_policy_violation",
        message: "Raw session event drill-down is blocked when IP masking is enabled",
        field: "sid",
      },
    });
  }
  // The log field settings say whether scoring is on; the column check only keeps the SQL off
  // files written before edge_score was logged.
  if (!(await cloudActiveLogFieldIds(serviceId, request.signal)).has("edge_score")) {
    return unavailableForRoute(route, "scoring_fields_not_present", {
      supported: false,
      sinceHours,
      sinceDays,
      threshold: previewThreshold,
      sid: eventMatch?.[1],
    });
  }
  const engine = cloudReadContext(request);
  const cols = await engine.getPresentColumns();
  if (!cols.has("edge_score")) {
    return unavailableForRoute(route, "no_scored_rows", {
      supported: true,
      sinceHours,
      sinceDays,
      threshold: previewThreshold,
      sid: eventMatch?.[1],
    });
  }
  const endText = url.searchParams.get("end_time") ?? url.searchParams.get("clamp_end");
  const end = endText ? Date.parse(endText) : Date.now();
  const startText = url.searchParams.get("start_time");
  const start = startText ? Date.parse(startText)
    : end - (route.startsWith("sessions/") ? sinceDays * 24 : sinceHours) * 3_600_000;
  if (!Number.isFinite(start) || !Number.isFinite(end) || start > end) {
    throw new Error("Invalid scoring time range");
  }
  const source = buildScoringSourceSql("requests", cols);
  const where = `ts_ms >= ${start} AND ts_ms <= ${end}`;
  const query = (sql: string) => engine.executeSql<Record<string, unknown>>(sql, [], request.signal);
  const hour = "strftime(timestamp, '%Y-%m-%dT%H:00:00Z')";
  const hasRtt = cols.has("edge_score_rtt_us");
  const hasExec = cols.has("edge_score_exec_us");
  const hasCompliance = cols.has("edge_cookie_compliance");
  const envelope = async (sql: string) => ({ rows: await query(sql), since_hours: sinceHours });
  const topLimit = bounded(url.searchParams.get("limit"), route === "analytics" ? 200 : 50, 500);
  const loadRange = options?.loadRange;
  const topPass1 = () => loadRange
    ? query(`SELECT timestamp::VARCHAR AS timestamp, ts_ms, edge_sid,
        edge_score, edge_score_l1, edge_score_l2, edge_cookie_compliance,
        edge_score_reason FROM ${source}
        WHERE ${where} AND edge_score IS NOT NULL ORDER BY edge_score DESC, timestamp DESC
        LIMIT ${topLimit}`)
    : query(`SELECT timestamp::VARCHAR AS timestamp, edge_sid,
        edge_score, edge_score_l1, edge_score_l2, edge_cookie_compliance,
        edge_score_reason, ip, ua, url, status, country FROM ${source}
        WHERE ${where} AND edge_score IS NOT NULL ORDER BY edge_score DESC, timestamp DESC
        LIMIT ${topLimit}`);
  const finishTop = async (rows: Record<string, unknown>[]) => ({
    rows: loadRange
      ? await enrichTopFlaggedRows(rows, loadRange, maskIps)
      : rows.map((r) => (maskIps ? { ...r, ip: maskIp(r.ip == null ? null : String(r.ip)) } : r)),
    since_hours: sinceHours,
  });
  const distribution = () => envelope(`SELECT ${hour} AS hour,
    CASE WHEN edge_score < 25 THEN '0-25' WHEN edge_score < 50 THEN '25-50'
      WHEN edge_score < 75 THEN '50-75' ELSE '75-100' END AS bucket, COUNT(*) AS count
    FROM ${source} WHERE ${where} AND edge_score IS NOT NULL GROUP BY 1, 2 ORDER BY 1, 2`);
  const compliance = () => hasCompliance
    ? envelope(`SELECT ${hour} AS hour, edge_cookie_compliance AS compliance, COUNT(*) AS count
      FROM ${source} WHERE ${where} AND edge_cookie_compliance IS NOT NULL
      AND edge_cookie_compliance != '' GROUP BY 1, 2 ORDER BY 1, 2`)
    : Promise.resolve(unavailableForRoute("compliance-breakdown", "cookie_compliance_not_recorded", { sinceHours }));
  const edgeOnly = cols.has("edge") ? "(edge = TRUE OR edge_score IS NOT NULL)" : "edge_score IS NOT NULL";
  const quantiles = [
    ...(hasRtt ? [0.5, 0.95, 0.99].map((p) =>
      `quantile_cont(edge_score_rtt_us, ${p}) AS rtt_p${p * 100}_us`) : []),
    ...(hasExec ? [0.5, 0.95].map((p) =>
      `quantile_cont(edge_score_exec_us, ${p}) AS exec_p${p * 100}_us`) : []),
  ];
  const latency = async () => hasRtt || hasExec
    ? { ...await envelope(`SELECT ${sinceHours <= 1 ? "strftime(timestamp, '%Y-%m-%dT%H:%M:00Z')" : hour} AS hour,
      COUNT(*) FILTER (WHERE edge_score IS NOT NULL) AS scored_count,
      COUNT(*) FILTER (WHERE contains(lower(edge_score_reason), 'compute-unavailable')) AS fail_open_count,
      COUNT(*) AS total_count, ${quantiles.join(", ")} FROM ${source}
      WHERE ${where} AND ${edgeOnly} GROUP BY 1 ORDER BY 1`),
      has_latency: true, granularity: sinceHours <= 1 ? "minute" : "hour" }
    : unavailableForRoute("latency-timeseries", "scorer_latency_not_recorded", { sinceHours });
  const health = async () => {
    const [row] = await query(`SELECT COUNT(*) AS total_edge_rows,
      COUNT(edge_score) AS scored_rows,
      COUNT(DISTINCT edge_sid) FILTER (WHERE edge_score IS NOT NULL AND edge_sid != '') AS distinct_sids,
      AVG(edge_score) AS avg_score, quantile_cont(edge_score, 0.5) AS p50_score,
      quantile_cont(edge_score, 0.95) AS p95_score, MAX(edge_score) AS max_score,
      COUNT(*) FILTER (WHERE edge_score IS NOT NULL AND contains(lower(edge_score_reason), 'compute-unavailable')) AS scorer_errors,
      COUNT(edge_score_l2) FILTER (WHERE edge_score IS NOT NULL) AS l2_evaluated,
      COUNT(*) FILTER (WHERE edge_score IS NOT NULL AND edge_score_l2 >= 50) AS l2_high_count
      ${quantiles.length ? `, ${quantiles.join(", ")}` : ""}
      FROM ${source} WHERE ${where} AND ${edgeOnly}`);
    const reasons = cols.has("edge_score_reason")
      ? await query(`SELECT trim(reason) AS reason, COUNT(*) AS count FROM ${source},
        unnest(string_split(edge_score_reason, ',')) AS reasons(reason)
        WHERE ${where} AND edge_score IS NOT NULL AND trim(reason) != ''
        GROUP BY 1 ORDER BY count DESC`) : null;
    const total = Number(row.total_edge_rows);
    const evaluated = Number(row.l2_evaluated);
    const high = Number(row.l2_high_count);
    const highPct = evaluated ? high / evaluated * 100 : null;
    return {
      ...row, since_hours: sinceHours,
      scored_rows: Number(row.scored_rows),
      distinct_sids: Number(row.distinct_sids),
      avg_score: row.avg_score == null ? null : Number(row.avg_score),
      p95_score: row.p95_score == null ? null : Number(row.p95_score),
      scorer_errors: cols.has("edge_score_reason") ? Number(row.scorer_errors) : null,
      fire_rate_pct: total ? Number(row.scored_rows) / total * 100 : null,
      fail_open_rate_pct: cols.has("edge_score_reason") && total ? Number(row.scorer_errors) / total * 100 : null,
      top_reasons: reasons?.slice(0, 10) ?? null,
      fail_open_breakdown: reasons?.filter((item) => /^(compute-unavailable|internal-error)/i.test(String(item.reason))) ?? null,
      latency: {
        available: hasRtt || hasExec,
        ...Object.fromEntries(["rtt_p50_us", "rtt_p95_us", "rtt_p99_us", "exec_p50_us", "exec_p95_us"]
          .map((key) => [key, row[key] ?? null])),
      },
      matrix_staleness: cols.has("edge_score_l2") ? {
        l2_evaluated: evaluated, l2_high_count: high, l2_high_pct: highPct,
        is_stale: evaluated >= 100 && Number(highPct) > 25, threshold_pct: 25,
      } : null,
      l2_enforce: null,
    };
  };
  const evaluateLabels = async (labels: ScoringLabel[]) => {
    const labelMap = new Map<string, string>();
    let nGood = 0;
    let nBad = 0;
    let nNeutral = 0;
    for (const item of labels) {
      const sid = labelSid(item);
      if (item.label === "good") nGood += 1;
      else if (item.label === "bad") nBad += 1;
      else if (item.label === "neutral") nNeutral += 1;
      if (sid && (item.label === "good" || item.label === "bad" || item.label === "neutral")) {
        labelMap.set(sid, item.label);
      }
    }
    const hasMinSamples =
      nGood >= SCORING_MIN_LABELS_PER_CLASS && nBad >= SCORING_MIN_LABELS_PER_CLASS;
    const base = emptyEvaluation(nGood, nBad, hasMinSamples, null);
    if (!hasMinSamples || !cols.has("edge_sid")) {
      return base;
    }

    const sids = Array.from(labelMap.keys()).map((sid) => duckSqlString(sid));
    if (sids.length === 0) return base;
    const scoreRows = await query(`SELECT edge_sid, MAX(edge_score) AS max_score,
      string_agg(COALESCE(edge_score_reason, ''), ',') AS reasons
      FROM ${source} WHERE ${where} AND edge_sid IN (${sids.join(", ")}) AND edge_score IS NOT NULL
      GROUP BY edge_sid`);

    const goodScores: number[] = [];
    const badScores: number[] = [];
    const reconstructed: Array<{ sid: string; label: string; score: number; atoms: Set<string> }> = [];
    for (const r of scoreRows) {
      const sid = String(r.edge_sid ?? "");
      const lbl = labelMap.get(sid);
      const score = Number(r.max_score);
      if (!lbl || !Number.isFinite(score)) continue;
      if (lbl === "good") goodScores.push(score);
      else if (lbl === "bad") badScores.push(score);
      const atoms = new Set(
        String(r.reasons ?? "")
          .split(",")
          .map((part) => part.trim())
          .filter(Boolean),
      );
      reconstructed.push({ sid, label: lbl, score, atoms });
    }

    const aucRaw = computeRocAuc(badScores, goodScores);
    const auc = aucRaw === null ? null : roundTo(aucRaw, 4);

    const buckets = SCORING_KNOWN_REASON_ATOMS.map((reason) => {
      const bucketGood: number[] = [];
      const bucketBad: number[] = [];
      for (const session of reconstructed) {
        if (!session.atoms.has(reason)) continue;
        if (session.label === "good") bucketGood.push(session.score);
        else if (session.label === "bad") bucketBad.push(session.score);
      }
      const bucketHasMin =
        bucketGood.length >= SCORING_MIN_LABELS_PER_CLASS &&
        bucketBad.length >= SCORING_MIN_LABELS_PER_CLASS;
      const bucketAucRaw = bucketHasMin ? computeRocAuc(bucketBad, bucketGood) : null;
      const bucketAuc = bucketAucRaw === null ? null : roundTo(bucketAucRaw, 4);
      return {
        reason,
        n_good: bucketGood.length,
        n_bad: bucketBad.length,
        min_per_class: SCORING_MIN_LABELS_PER_CLASS,
        has_min_samples: bucketHasMin,
        ...(bucketHasMin && bucketAuc !== null
          ? { auc: bucketAuc, passed: bucketAuc >= 0.85, threshold: 0.85 }
          : {}),
      };
    });

    const scoredBinary = reconstructed.filter((r) => r.label === "good" || r.label === "bad");
    let curves = {
      has_min_samples: false,
      min_per_class: SCORING_MIN_LABELS_PER_CLASS,
      n_good: nGood,
      n_bad: nBad,
      n_labels_total: labels.length,
      note: "labels exist but none of their sids have landed in parquet yet" as string | undefined,
      auc: null as number | null,
      average_precision: null as number | null,
      roc: [] as Array<{ threshold: number; fpr: number; tpr: number }>,
      pr: [] as Array<{ threshold: number; precision: number; recall: number }>,
    };
    if (scoredBinary.length > 0 && goodScores.length > 0 && badScores.length > 0) {
      const totalPos = badScores.length;
      const totalNeg = goodScores.length;
      const roc: Array<{ threshold: number; fpr: number; tpr: number }> = [];
      const pr: Array<{ threshold: number; precision: number; recall: number }> = [];
      for (let t = 100; t >= 0; t--) {
        let tp = 0;
        let fp = 0;
        for (const item of scoredBinary) {
          if (item.score < t) continue;
          if (item.label === "bad") tp += 1;
          else if (item.label === "good") fp += 1;
        }
        const tpr = tp / totalPos;
        const fpr = fp / totalNeg;
        const precision = tp + fp > 0 ? tp / (tp + fp) : 1.0;
        roc.push({ threshold: t, fpr: roundTo(fpr, 4), tpr: roundTo(tpr, 4) });
        pr.push({ threshold: t, precision: roundTo(precision, 4), recall: roundTo(tpr, 4) });
      }
      curves = {
        has_min_samples: true,
        min_per_class: SCORING_MIN_LABELS_PER_CLASS,
        n_good: totalNeg,
        n_bad: totalPos,
        n_labels_total: labels.length,
        note: undefined,
        auc: roundTo(trapezoidArea(roc.map((p) => [p.fpr, p.tpr])), 4),
        average_precision: roundTo(trapezoidArea(pr.map((p) => [p.recall, p.precision])), 4),
        roc,
        pr,
      };
    }

    return {
      evaluation: {
        n_good: nGood,
        n_bad: nBad,
        n_neutral: nNeutral,
        n_reconstructed: reconstructed.length,
        n_labels_total: labels.length,
        min_per_class: SCORING_MIN_LABELS_PER_CLASS,
        has_min_samples: hasMinSamples,
        auc,
        threshold: 0.85,
        default_min_auc: 0.85,
        passed: auc !== null && auc >= 0.85,
      },
      evaluation_per_reason: {
        buckets,
        n_good: nGood,
        n_bad: nBad,
        min_per_class: SCORING_MIN_LABELS_PER_CLASS,
        known_reasons: [...SCORING_KNOWN_REASON_ATOMS],
        has_min_samples_overall: hasMinSamples,
      },
      curves,
    };
  };
  if (route === "analytics") {
    const [topBaseRows, scoreDistribution, complianceBreakdown, latencyTimeseries, snapshot, labels] =
      await Promise.all([
        topPass1(), distribution(), compliance(), latency(), health(),
        fetchScoringLabels(serviceId ?? "", request.signal),
      ]);
    const evalBundle = await evaluateLabels(labels);
    // Pass-2 enrichment runs after all Pass-1 queries finish so DuckDB's active window isn't mutated mid-scan.
    const topFlagged = await finishTop(topBaseRows);
    return {
      available: true as const, capabilities: CLOUD_SCORING_CAPABILITIES,
      top_flagged: topFlagged, score_distribution: scoreDistribution,
      compliance_breakdown: complianceBreakdown, latency_timeseries: latencyTimeseries, health: snapshot,
      evaluation: evalBundle.evaluation,
      evaluation_per_reason: evalBundle.evaluation_per_reason,
    };
  }
  if (route === "top-flagged") return finishTop(await topPass1());
  if (route === "score-distribution") return distribution();
  if (route === "compliance-breakdown") return compliance();
  if (route === "latency-timeseries") return latency();
  if (route === "health") return health();
  if (route === "threshold-preview") {
    const [sessionRows, labels] = await Promise.all([
      query(`SELECT edge_sid, MAX(edge_score) AS max_score
        FROM ${source}
        WHERE ${where} AND edge_score IS NOT NULL AND edge_sid IS NOT NULL AND edge_sid != ''
        GROUP BY edge_sid`),
      fetchScoringLabels(serviceId ?? "", request.signal),
    ]);
    const labelMap = new Map<string, string>();
    for (const item of labels) {
      const sid = labelSid(item);
      if (sid && (item.label === "good" || item.label === "bad")) {
        labelMap.set(sid, item.label);
      }
    }
    const total = sessionRows.length;
    let flaggedTotal = 0;
    let flaggedGood = 0;
    let flaggedBad = 0;
    let passedGood = 0;
    let passedBad = 0;
    for (const row of sessionRows) {
      const sid = String(row.edge_sid ?? "");
      const score = Number(row.max_score);
      const lbl = labelMap.get(sid);
      if (score >= previewThreshold) {
        flaggedTotal += 1;
        if (lbl === "good") flaggedGood += 1;
        else if (lbl === "bad") flaggedBad += 1;
      } else {
        if (lbl === "good") passedGood += 1;
        else if (lbl === "bad") passedBad += 1;
      }
    }
    const passedTotal = Math.max(0, total - flaggedTotal);
    const flaggedUnlabeled = Math.max(0, flaggedTotal - (flaggedGood + flaggedBad));
    const passedUnlabeled = Math.max(0, passedTotal - (passedGood + passedBad));
    const flaggedLabeled = flaggedGood + flaggedBad;
    const allLabeledBad = flaggedBad + passedBad;
    return {
      threshold: previewThreshold,
      since_hours: sinceHours,
      total_scored_sessions: total,
      flagged: {
        total: flaggedTotal,
        good: flaggedGood,
        bad: flaggedBad,
        unlabeled: flaggedUnlabeled,
      },
      passed: {
        good: passedGood,
        bad: passedBad,
        unlabeled: passedUnlabeled,
      },
      precision: flaggedLabeled > 0 ? roundTo(flaggedBad / flaggedLabeled, 4) : null,
      recall: allLabeledBad > 0 ? roundTo(flaggedBad / allLabeledBad, 4) : null,
    };
  }
  if (route === "evaluation" || route === "evaluation/per-reason" || route === "curves") {
    const labels = await fetchScoringLabels(serviceId ?? "", request.signal);
    const evalBundle = await evaluateLabels(labels);
    if (route === "evaluation/per-reason") return evalBundle.evaluation_per_reason;
    if (route === "curves") return evalBundle.curves;
    return evalBundle.evaluation;
  }
  if (eventMatch) {
    if (!cols.has("edge_sid")) {
      return unavailableForRoute(route, "edge_session_ids_not_recorded", {
        supported: false,
        sinceHours,
        sinceDays,
        sid: eventMatch[1],
      });
    }
    let sid = eventMatch[1];
    try {
      sid = decodeURIComponent(eventMatch[1]);
    } catch {
      return unavailableForRoute(route, "invalid_session_id", {
        supported: false,
        sinceHours,
        sinceDays,
        sid,
      });
    }
    const rows = await query(`SELECT timestamp::VARCHAR AS ts, edge_score, edge_score_l1,
      edge_score_l2, edge_score_reason, edge_cookie_compliance, url, status FROM ${source}
      WHERE ${where} AND edge_sid = ${duckSqlString(sid)}
      ORDER BY timestamp ASC LIMIT 500`);
    return { sid, since_days: sinceDays, event_count: rows.length, events: rows };
  }
  throw new Error("Unsupported scoring scan route");
}

export async function cloudScoringRead(
  request: Request,
  options?: ScoringReadOptions,
): Promise<Response | null> {
  const match = new URL(request.url).pathname.match(/^\/api\/services\/([^/]+)\/scoring\/(.+)$/);
  if (!match || request.method !== "GET") return null;
  const [, , route] = match;
  if (!CLOUD_SCORING_SCAN_ROUTES.has(route) && !/^sessions\/[^/]+\/events$/.test(route)) return null;
  try {
    return Response.json(await queryCloudScoring(request, route, options));
  } catch (err) {
    if (err instanceof CloudScoringHttpError) {
      return Response.json(err.body, { status: err.status });
    }
    throw err;
  }
}
