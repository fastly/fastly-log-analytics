import { cloudReadContext } from "./range-loader";
import { buildScoringSourceSql } from "./sessions";
import { escapeSqlLiteral } from "./sql-validator";
import { useSessionRoleStore } from "@/stores/filterStore";

export const CLOUD_SCORING_CAPABILITIES = {
  historical_analytics: true,
  management: false,
  labels: false,
  evaluation: false,
  thresholds: false,
  matrix_versions: false,
  audit: false,
} as const;

const CONTROL_READS = new Set([
  "evaluation", "evaluation/per-reason", "curves", "labels", "threshold",
  "threshold-preview", "enforce-threshold", "exclude-regex", "enforce-status-code", "l2-enforce",
  "matrix/versions", "matrix-versions", "audit",
]);

export const CLOUD_SCORING_SCAN_ROUTES = new Set([
  "analytics", "top-flagged", "score-distribution", "compliance-breakdown",
  "latency-timeseries", "health",
]);

function unavailable(reason: string) {
  return { supported: false as const, available: false as const, reason };
}

function bounded(value: string | null, fallback: number, max: number): number {
  const parsed = Number(value ?? fallback);
  return Number.isFinite(parsed) ? Math.min(max, Math.max(1, Math.floor(parsed))) : fallback;
}

export async function queryCloudScoring(request: Request, route: string) {
  const engine = cloudReadContext(request);
  const cols = await engine.getPresentColumns();
  if (!cols.has("edge_score")) return unavailable("scoring_fields_not_present");
  const url = new URL(request.url);
  const maskIps = request.headers.get("x-mask-ips") === "1" ||
    request.headers.get("x-session-role") === "analyst-no-pii" ||
    ["1", "true"].includes(url.searchParams.get("mask_ips") ?? "") ||
    useSessionRoleStore.getState().isRemoteAnalyst;
  const sinceHours = bounded(url.searchParams.get("since_hours"), 24, 168);
  const sinceDays = bounded(url.searchParams.get("since_days"), 30, 90);
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
  const query = (sql: string) => engine.executeSql(sql, [], request.signal);
  const hour = "strftime(timestamp, '%Y-%m-%dT%H:00:00Z')";
  const hasRtt = cols.has("edge_score_rtt_us");
  const hasExec = cols.has("edge_score_exec_us");
  const hasCompliance = cols.has("edge_cookie_compliance");
  const envelope = async (sql: string) => ({ rows: await query(sql), since_hours: sinceHours });
  const top = () => envelope(`SELECT timestamp::VARCHAR AS timestamp, edge_sid,
    edge_score, edge_score_l1, edge_score_l2, edge_cookie_compliance,
    edge_score_reason, ${maskIps ? "'0.0.0.0' AS ip" : "ip"}, ua, url, status, country FROM ${source}
    WHERE ${where} AND edge_score IS NOT NULL ORDER BY edge_score DESC, timestamp DESC
    LIMIT ${bounded(url.searchParams.get("limit"), route === "analytics" ? 200 : 50, 500)}`);
  const distribution = () => envelope(`SELECT ${hour} AS hour,
    CASE WHEN edge_score < 25 THEN '0-25' WHEN edge_score < 50 THEN '25-50'
      WHEN edge_score < 75 THEN '50-75' ELSE '75-100' END AS bucket, COUNT(*) AS count
    FROM ${source} WHERE ${where} AND edge_score IS NOT NULL GROUP BY 1, 2 ORDER BY 1, 2`);
  const compliance = () => hasCompliance
    ? envelope(`SELECT ${hour} AS hour, edge_cookie_compliance AS compliance, COUNT(*) AS count
      FROM ${source} WHERE ${where} AND edge_cookie_compliance IS NOT NULL
      AND edge_cookie_compliance != '' GROUP BY 1, 2 ORDER BY 1, 2`)
    : Promise.resolve(unavailable("cookie_compliance_not_recorded"));
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
    : unavailable("scorer_latency_not_recorded");
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
  if (route === "analytics") {
    const [topFlagged, scoreDistribution, complianceBreakdown, latencyTimeseries, snapshot] =
      await Promise.all([top(), distribution(), compliance(), latency(), health()]);
    return {
      available: true, capabilities: CLOUD_SCORING_CAPABILITIES,
      top_flagged: topFlagged, score_distribution: scoreDistribution,
      compliance_breakdown: complianceBreakdown, latency_timeseries: latencyTimeseries, health: snapshot,
    };
  }
  if (route === "top-flagged") return top();
  if (route === "score-distribution") return distribution();
  if (route === "compliance-breakdown") return compliance();
  if (route === "latency-timeseries") return latency();
  if (route === "health") return health();
  const events = route.match(/^sessions\/([^/]+)\/events$/);
  if (events) {
    if (!cols.has("edge_sid")) return unavailable("edge_session_ids_not_recorded");
    const sid = decodeURIComponent(events[1]);
    const rows = await query(`SELECT timestamp::VARCHAR AS ts, edge_score, edge_score_l1,
      edge_score_l2, edge_score_reason, edge_cookie_compliance, url, status FROM ${source}
      WHERE ${where} AND edge_sid = '${escapeSqlLiteral(sid)}'
      ORDER BY timestamp ASC LIMIT 500`);
    return { sid, since_days: sinceDays, event_count: rows.length, events: rows };
  }
  throw new Error("Unsupported scoring scan route");
}

export async function cloudScoringRead(request: Request): Promise<Response | null> {
  const match = new URL(request.url).pathname.match(/^\/api\/services\/([^/]+)\/scoring\/(.+)$/);
  if (!match || request.method !== "GET") return null;
  const [, encodedService, route] = match;
  if (route === "config" || route === "status") {
    const response = await fetch("/api/services", { signal: request.signal });
    if (!response.ok) return response;
    const payload = await response.json() as { services?: Array<{
      service_id: string; scoring?: { enabled?: boolean }; scoring_enabled?: boolean;
    }> };
    const serviceId = decodeURIComponent(encodedService);
    const service = payload.services?.find((item) => item.service_id === serviceId);
    if (!service) return Response.json({ detail: { error: "service_not_found" } }, { status: 404 });
    const status = {
      service_id: serviceId,
      enabled: service.scoring?.enabled === true || service.scoring_enabled === true,
      supported: false, drift_known: false,
      reason: "cloud_scoring_management_unsupported", capabilities: CLOUD_SCORING_CAPABILITIES,
    };
    return Response.json(route === "config" ? { status, capabilities: CLOUD_SCORING_CAPABILITIES } : status);
  }
  if (CONTROL_READS.has(route)) {
    return Response.json({ ...unavailable("cloud_scoring_control_plane_unavailable"), capabilities: CLOUD_SCORING_CAPABILITIES });
  }
  if (!CLOUD_SCORING_SCAN_ROUTES.has(route) && !/^sessions\/[^/]+\/events$/.test(route)) return null;
  return Response.json(await queryCloudScoring(request, route));
}
