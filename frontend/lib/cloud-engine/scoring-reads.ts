import { cloudActiveLogFieldIds } from "./active-log-fields";
import { cloudReadContext } from "./range-loader";
import { computeRocAuc } from "./scoring-train";
import { buildScoringSourceSql } from "./sessions";
import { duckSqlString } from "./dashboard/duckdb";
import { useSessionRoleStore } from "@/stores/filterStore";

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

/** Every column a scoring scan reads; optional edge_score* fields are skipped in files that lack them. */
export const SCORING_SCAN_COLUMNS = [
  "timestamp", "ts_ms", "edge", "edge_sid", "edge_score", "edge_score_l1", "edge_score_l2", "edge_score_reason",
  "edge_cookie_compliance", "edge_score_rtt_us", "edge_score_exec_us", "ip", "ua", "url", "status", "country",
] as const;

export function scoringScanColumns(): string[] {
  return [...SCORING_SCAN_COLUMNS];
}

function unavailable(reason: string) {
  return { supported: false as const, available: false as const, reason };
}

function bounded(value: string | null, fallback: number, max: number): number {
  const parsed = Number(value ?? fallback);
  return Number.isFinite(parsed) ? Math.min(max, Math.max(1, Math.floor(parsed))) : fallback;
}

export async function queryCloudScoring(request: Request, route: string) {
  const url = new URL(request.url);
  const serviceId = decodeURIComponent(url.pathname.split("/")[3] ?? "") || undefined;
  // The log field settings say whether scoring is on; the column check only keeps the SQL off
  // files written before edge_score was logged.
  if (!(await cloudActiveLogFieldIds(serviceId, request.signal)).has("edge_score")) {
    return unavailable("scoring_fields_not_present");
  }
  const engine = cloudReadContext(request);
  const cols = await engine.getPresentColumns();
  if (!cols.has("edge_score")) return { supported: true as const, available: false as const, reason: "no_scored_rows" };
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
  if (route === "threshold-preview") {
    const threshold = bounded(url.searchParams.get("threshold"), 50, 100);
    const [row] = await query(`SELECT COUNT(*) AS total_scored,
      COUNT(*) FILTER (WHERE edge_score >= ${threshold}) AS flagged_count
      FROM ${source} WHERE ${where} AND edge_score IS NOT NULL`);
    const total = Number(row?.total_scored ?? 0);
    const flagged = Number(row?.flagged_count ?? 0);
    return {
      threshold,
      since_hours: sinceHours,
      total_scored: total,
      flagged_count: flagged,
      flagged_pct: total > 0 ? (flagged / total) * 100 : 0,
    };
  }
  if (route === "evaluation" || route === "evaluation/per-reason" || route === "curves") {
    const match = url.pathname.match(/^\/api\/services\/([^/]+)\/scoring\//);
    let serviceId = "";
    if (match) {
      try {
        serviceId = decodeURIComponent(match[1]);
      } catch {
        serviceId = "";
      }
    }
    let labels: Array<{ edge_sid: string; label: string }> = [];
    if (serviceId) {
      const resp = await fetch(`/api/services/${encodeURIComponent(serviceId)}/scoring/labels`, {
        signal: request.signal,
      });
      if (resp.ok) {
        const data = (await resp.json()) as { labels?: Array<{ edge_sid: string; label: string }> };
        labels = data.labels ?? [];
      }
    }
    const labelMap = new Map(labels.map((l) => [l.edge_sid, l.label]));
    const goodSids = labels.filter((l) => l.label === "good");
    const badSids = labels.filter((l) => l.label === "bad");
    const hasMinSamples = goodSids.length >= 5 && badSids.length >= 5;
    let auc: number | null = null;
    if (hasMinSamples && cols.has("edge_sid")) {
      const sids = labels
        .filter((l) => l.label === "good" || l.label === "bad")
        .map((l) => duckSqlString(l.edge_sid))
        .join(", ");
      const scoreRows = await query(`SELECT edge_sid, MAX(edge_score) AS max_score
        FROM ${source} WHERE ${where} AND edge_sid IN (${sids}) AND edge_score IS NOT NULL
        GROUP BY edge_sid`);
      const goodScores: number[] = [];
      const badScores: number[] = [];
      for (const r of scoreRows) {
        const lbl = labelMap.get(String(r.edge_sid));
        if (lbl === "good") goodScores.push(Number(r.max_score));
        else if (lbl === "bad") badScores.push(Number(r.max_score));
      }
      auc = computeRocAuc(badScores, goodScores);
    }
    if (route === "evaluation/per-reason") {
      return { rows: [], n_good: goodSids.length, n_bad: badSids.length, has_min_samples: hasMinSamples };
    }
    if (route === "curves") {
      return {
        available: hasMinSamples && auc !== null,
        n_good: goodSids.length,
        n_bad: badSids.length,
        auc,
        roc: [],
        pr: [],
      };
    }
    return {
      n_good: goodSids.length,
      n_bad: badSids.length,
      min_per_class: 5,
      has_min_samples: hasMinSamples,
      auc,
      threshold: 0.85,
      passed: auc !== null && auc >= 0.85,
    };
  }
  const events = route.match(/^sessions\/([^/]+)\/events$/);
  if (events) {
    if (!cols.has("edge_sid")) return unavailable("edge_session_ids_not_recorded");
    let sid = events[1];
    try {
      sid = decodeURIComponent(events[1]);
    } catch {
      return unavailable("invalid_session_id");
    }
    const rows = await query(`SELECT timestamp::VARCHAR AS ts, edge_score, edge_score_l1,
      edge_score_l2, edge_score_reason, edge_cookie_compliance, url, status FROM ${source}
      WHERE ${where} AND edge_sid = ${duckSqlString(sid)}
      ORDER BY timestamp ASC LIMIT 500`);
    return { sid, since_days: sinceDays, event_count: rows.length, events: rows };
  }
  throw new Error("Unsupported scoring scan route");
}

export async function cloudScoringRead(request: Request): Promise<Response | null> {
  const match = new URL(request.url).pathname.match(/^\/api\/services\/([^/]+)\/scoring\/(.+)$/);
  if (!match || request.method !== "GET") return null;
  const [, , route] = match;
  if (!CLOUD_SCORING_SCAN_ROUTES.has(route) && !/^sessions\/[^/]+\/events$/.test(route)) return null;
  return Response.json(await queryCloudScoring(request, route));
}
