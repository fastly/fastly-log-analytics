import {
  getDuckDBEngine,
  type DuckDBAnalyticalEngine,
} from "./dashboard/duckdb";

export const TCP_FIELDS = [
  "client_rtt_us",
  "client_ploss",
  "client_cwnd",
  "client_tcpi_rtt",
  "client_tcpi_rttvar",
  "client_tcpi_rcv_space",
  "client_tcpi_snd_mss",
  "client_tcpi_delta_retrans",
] as const;

export const PROTOCOL_BUCKETS = [
  "HTTP/3",
  "HTTP/2",
  "HTTP/1.1",
  "HTTP/1.0",
  "Other",
] as const;

export interface MetroRow {
  city?: string | null;
  metro?: string | null;
  country?: string | null;
  reqs?: number;
  total_reqs?: number;
  health_score?: number;
  rtt_med_us?: number;
  rtt_med_ms?: number;
  avg_ploss?: number;
  avg_ploss_pct?: number;
  error_pct?: number;
  [key: string]: unknown;
}

export interface NetworkQueryParams {
  start_time?: string | number;
  end_time?: string | number;
  service_id?: string;
  filter_country?: string;
  filter_asn?: string | number;
  filter_pop?: string;
  engine?: Pick<DuckDBAnalyticalEngine, "executeSql" | "getPresentColumns">;
  signal?: AbortSignal;
}

function clamp(val: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, val));
}

function roundTo(val: number, decimals: number): number {
  const factor = 10 ** decimals;
  return Math.round(val * factor) / factor;
}

/**
 * Computes composite 0-100 network health score.
 * Supports both:
 *  - 4-arg: (rtt_med_us, rtt_baseline_us, avg_ploss, error_pct)
 *  - 5-arg: (throughput_bps, rtt_congestion_us, avg_ploss, rtt_jitter_us, error_pct)
 */
export function computeNetworkHealthScore(
  arg1: number | null | undefined,
  arg2: number | null | undefined,
  avg_ploss: number | null | undefined,
  arg4: number | null | undefined,
  arg5?: number | null | undefined,
): number {
  if (arg5 === undefined) {
    const rtt_med_us = Math.max(0, Number(arg1 ?? 0));
    const rtt_baseline_us = Math.max(1, Number(arg2 ?? 1));
    const ploss = Math.max(0, Number(avg_ploss ?? 0));
    const error_pct = Math.max(0, Number(arg4 ?? 0));

    const rtt_ratio = rtt_med_us / rtt_baseline_us;
    const rtt_score = clamp(100 - Math.max(0, rtt_ratio - 1) * 50, 0, 100);
    const ploss_pct = ploss <= 1 ? ploss * 100 : ploss;
    const ploss_score = clamp(100 - ploss_pct * 20, 0, 100);
    const error_score = clamp(100 - error_pct * 10, 0, 100);
    const composite = 0.4 * rtt_score + 0.35 * ploss_score + 0.25 * error_score;
    return roundTo(clamp(composite, 0, 100), 1);
  }

  const throughput_bps = Math.max(0, Number(arg1 ?? 0));
  const rtt_congestion_us = Math.max(0, Number(arg2 ?? 0));
  const ploss = Math.max(0, Number(avg_ploss ?? 0));
  const rtt_jitter_us = Math.max(0, Number(arg4 ?? 0));
  const error_pct = Math.max(0, Number(arg5 ?? 0));

  const throughput_mbps = throughput_bps / 1_000_000;
  const throughput_score = clamp((throughput_mbps / 50) * 100, 0, 100);
  const congestion_ms = rtt_congestion_us / 1000;
  const congestion_score = clamp(100 - (congestion_ms / 100) * 100, 0, 100);
  const ploss_pct = ploss <= 1 ? ploss * 100 : ploss;
  const ploss_score = clamp(100 - (ploss_pct / 5) * 100, 0, 100);
  const jitter_ms = rtt_jitter_us / 1000;
  const jitter_score = clamp(100 - (jitter_ms / 50) * 100, 0, 100);
  const error_score = clamp(100 - (error_pct / 10) * 100, 0, 100);
  const composite =
    0.25 * throughput_score +
    0.25 * congestion_score +
    0.25 * ploss_score +
    0.15 * jitter_score +
    0.1 * error_score;
  return roundTo(clamp(composite, 0, 100), 1);
}

export const compute_network_health_score = computeNetworkHealthScore;

/**
 * Collapses duplicate (city, country) rows in the metro leaderboard by
 * summing request counts and computing request-weighted averages.
 */
export function deduplicateMetroLeaderboard(
  rows: readonly MetroRow[],
): MetroRow[] {
  const byKey = new Map<string, MetroRow & { city: string; country: string; reqs: number; total_reqs: number }>();
  const weightedFields = [
    "health_score",
    "rtt_med_us",
    "rtt_med_ms",
    "avg_ploss",
    "avg_ploss_pct",
    "error_pct",
  ] as const;

  for (const row of rows) {
    const city = String(row.city ?? row.metro ?? "").trim();
    const country = String(row.country ?? "").trim();
    if (!city) continue;
    const key = `${city.toLowerCase()}::${country.toLowerCase()}`;
    const reqs = Math.max(0, Number(row.reqs ?? row.total_reqs ?? 0));
    const existing = byKey.get(key);
    if (!existing) {
      const copy: MetroRow & {
        city: string;
        country: string;
        reqs: number;
        total_reqs: number;
      } = {
        ...row,
        city,
        country,
        reqs,
        total_reqs: reqs,
      };
      if ("metro" in row) {
        copy.metro = city;
      }
      byKey.set(key, copy);
    } else {
      const prevReqs = existing.reqs;
      const totalReqs = prevReqs + reqs;
      for (const field of weightedFields) {
        if (field in row || field in existing) {
          const prevVal = Number(existing[field] ?? 0);
          const curVal = Number(row[field] ?? 0);
          existing[field] =
            totalReqs > 0
              ? roundTo((prevVal * prevReqs + curVal * reqs) / totalReqs, 2)
              : 0;
        }
      }
      existing.reqs = totalReqs;
      existing.total_reqs = totalReqs;
    }
  }

  return [...byKey.values()]
    .sort((a, b) => b.reqs - a.reqs)
    .slice(0, 200);
}

export const deduplicate_metro_leaderboard = deduplicateMetroLeaderboard;

export function normalizeProtocolBucket(raw: unknown): string {
  const val = String(raw ?? "").trim().toUpperCase();
  if (val === "HTTP/3" || val === "H3" || val.startsWith("HTTP/3")) {
    return "HTTP/3";
  }
  if (val === "HTTP/2" || val === "H2" || val === "HTTP/2.0") {
    return "HTTP/2";
  }
  if (val === "HTTP/1.1") {
    return "HTTP/1.1";
  }
  if (val === "HTTP/1.0") {
    return "HTTP/1.0";
  }
  return "Other";
}

export function emptyNetworkHealth(missingTcpFields: string[] = []) {
  return {
    summary: {
      total_requests: 0,
      avg_rtt_ms: 0,
      median_rtt_ms: 0,
      p95_rtt_ms: 0,
      avg_ploss_pct: 0,
      health_score: 100,
    },
    timeseries: [] as Record<string, unknown>[],
    by_country: [] as Record<string, unknown>[],
    by_asn: [] as Record<string, unknown>[],
    by_pop: [] as Record<string, unknown>[],
    metro_leaderboard: [] as MetroRow[],
    missing_tcp_fields: missingTcpFields,
  };
}

export function emptyNetworkQuality(missingTcpFields: string[] = []) {
  return {
    summary: {
      total_requests: 0,
      avg_throughput_mbps: 0,
      avg_cwnd: 0,
      avg_rtt_ms: 0,
      avg_rttvar_ms: 0,
      retrans_rate_pct: 0,
    },
    scatter: [] as Record<string, unknown>[],
    by_region: [] as Record<string, unknown>[],
    by_asn: [] as Record<string, unknown>[],
    protocol_distribution: PROTOCOL_BUCKETS.map((protocol) => ({
      protocol,
      requests: 0,
      pct: 0,
      avg_rtt_ms: 0,
    })),
    missing_tcp_fields: missingTcpFields,
  };
}

export function emptyPopHealth() {
  return {
    pops: [] as Record<string, unknown>[],
    by_pop: [] as Record<string, unknown>[],
    elevated_count: 0,
    total_pops: 0,
  };
}

export function emptyNetworkBundle(missingTcpFields: string[] = []) {
  return {
    network_health: emptyNetworkHealth(missingTcpFields),
    network_quality: emptyNetworkQuality(missingTcpFields),
    pop_health: emptyPopHealth(),
    missing_tcp_fields: missingTcpFields,
  };
}

function parseEpochMs(val: string | number | undefined): number | null {
  if (val === undefined || val === null || val === "") return null;
  if (typeof val === "number") {
    return val < 1e12 ? Math.floor(val * 1000) : Math.floor(val);
  }
  const num = Number(val);
  if (Number.isFinite(num)) {
    return num < 1e12 ? Math.floor(num * 1000) : Math.floor(num);
  }
  const parsed = Date.parse(val);
  return Number.isFinite(parsed) ? parsed : null;
}

function buildWhereClause(params: NetworkQueryParams): string {
  const clauses: string[] = ["1=1"];
  const startMs = parseEpochMs(params.start_time);
  const endMs = parseEpochMs(params.end_time);
  if (startMs !== null) {
    clauses.push(`ts_ms >= ${startMs}`);
  }
  if (endMs !== null) {
    clauses.push(`ts_ms <= ${endMs}`);
  }
  if (params.service_id) {
    const escaped = String(params.service_id).replaceAll("'", "''");
    clauses.push(`(service_id = '${escaped}' OR service_id IS NULL)`);
  }
  if (params.filter_country) {
    const escaped = String(params.filter_country).replaceAll("'", "''");
    clauses.push(`COALESCE(geo_country, country) = '${escaped}'`);
  }
  if (params.filter_asn !== undefined && params.filter_asn !== "") {
    const asnNum = Number(params.filter_asn);
    if (Number.isFinite(asnNum)) {
      clauses.push(`COALESCE(client_asn, asn) = ${Math.floor(asnNum)}`);
    }
  }
  if (params.filter_pop) {
    const escaped = String(params.filter_pop).replaceAll("'", "''");
    clauses.push(`COALESCE(fastly_pop, pop) = '${escaped}'`);
  }
  return clauses.join(" AND ");
}

async function detectMissingTcpFields(
  engine: Pick<DuckDBAnalyticalEngine, "getPresentColumns">,
): Promise<string[]> {
  const present = await engine.getPresentColumns();
  if (present.size === 0) {
    return [...TCP_FIELDS];
  }
  return TCP_FIELDS.filter((f) => !present.has(f));
}

export async function queryNetworkHealth(params: NetworkQueryParams = {}) {
  const engine = params.engine ?? getDuckDBEngine();
  const missingTcpFields = await detectMissingTcpFields(engine);
  const where = buildWhereClause(params);

  const summaryRows = await engine.executeSql<{
    total_requests: number;
    avg_rtt_us: number | null;
    med_rtt_us: number | null;
    p95_rtt_us: number | null;
    p10_rtt_us: number | null;
    avg_ploss: number | null;
    error_pct: number | null;
  }>(
    `SELECT
      COUNT(*)::BIGINT AS total_requests,
      AVG(COALESCE(client_rtt_us, client_tcpi_rtt))::DOUBLE AS avg_rtt_us,
      QUANTILE_CONT(COALESCE(client_rtt_us, client_tcpi_rtt), 0.5)::DOUBLE AS med_rtt_us,
      QUANTILE_CONT(COALESCE(client_rtt_us, client_tcpi_rtt), 0.95)::DOUBLE AS p95_rtt_us,
      QUANTILE_CONT(COALESCE(client_rtt_us, client_tcpi_rtt), 0.1)::DOUBLE AS p10_rtt_us,
      AVG(client_ploss)::DOUBLE AS avg_ploss,
      (COUNT(*) FILTER (WHERE status >= 500 AND status < 600) * 100.0 / NULLIF(COUNT(*), 0))::DOUBLE AS error_pct
    FROM requests
    WHERE ${where}`,
    [],
    params.signal,
  );

  const totalRequests = Number(summaryRows[0]?.total_requests ?? 0);
  if (totalRequests === 0) {
    return emptyNetworkHealth(missingTcpFields);
  }

  const s = summaryRows[0];
  const medRttUs = Number(s.med_rtt_us ?? 0);
  const baseRttUs = Math.max(1, Number(s.p10_rtt_us ?? medRttUs ?? 1));
  const avgPloss = Number(s.avg_ploss ?? 0);
  const errorPct = Number(s.error_pct ?? 0);
  const healthScore = computeNetworkHealthScore(
    medRttUs,
    baseRttUs,
    avgPloss,
    errorPct,
  );

  const byCountryRows = await engine.executeSql<Record<string, unknown>>(
    `SELECT
      COALESCE(geo_country, country, 'Unknown') AS country,
      COUNT(*)::BIGINT AS reqs,
      COUNT(*)::BIGINT AS total_reqs,
      QUANTILE_CONT(COALESCE(client_rtt_us, client_tcpi_rtt), 0.5)::DOUBLE AS rtt_med_us,
      QUANTILE_CONT(COALESCE(client_rtt_us, client_tcpi_rtt), 0.1)::DOUBLE AS rtt_base_us,
      AVG(client_ploss)::DOUBLE AS avg_ploss,
      (COUNT(*) FILTER (WHERE status >= 500 AND status < 600) * 100.0 / NULLIF(COUNT(*), 0))::DOUBLE AS error_pct
    FROM requests
    WHERE ${where}
    GROUP BY 1
    ORDER BY reqs DESC
    LIMIT 100`,
    [],
    params.signal,
  );

  const by_country = byCountryRows.map((row) => {
    const rttMedUs = Number(row.rtt_med_us ?? 0);
    const rttBaseUs = Math.max(1, Number(row.rtt_base_us ?? rttMedUs ?? 1));
    const ploss = Number(row.avg_ploss ?? 0);
    const errPct = Number(row.error_pct ?? 0);
    return {
      country: String(row.country ?? "Unknown"),
      reqs: Number(row.reqs ?? 0),
      total_reqs: Number(row.total_reqs ?? 0),
      rtt_med_us: roundTo(rttMedUs, 1),
      rtt_med_ms: roundTo(rttMedUs / 1000, 2),
      avg_ploss: roundTo(ploss, 4),
      avg_ploss_pct: roundTo(ploss <= 1 ? ploss * 100 : ploss, 2),
      error_pct: roundTo(errPct, 2),
      health_score: computeNetworkHealthScore(
        rttMedUs,
        rttBaseUs,
        ploss,
        errPct,
      ),
    };
  });

  const byAsnRows = await engine.executeSql<Record<string, unknown>>(
    `SELECT
      COALESCE(client_asn, asn, 0)::BIGINT AS asn,
      COALESCE(as_name, 'AS' || CAST(COALESCE(client_asn, asn, 0) AS VARCHAR)) AS as_name,
      COUNT(*)::BIGINT AS reqs,
      QUANTILE_CONT(COALESCE(client_rtt_us, client_tcpi_rtt), 0.5)::DOUBLE AS rtt_med_us,
      QUANTILE_CONT(COALESCE(client_rtt_us, client_tcpi_rtt), 0.1)::DOUBLE AS rtt_base_us,
      AVG(client_ploss)::DOUBLE AS avg_ploss,
      (COUNT(*) FILTER (WHERE status >= 500 AND status < 600) * 100.0 / NULLIF(COUNT(*), 0))::DOUBLE AS error_pct
    FROM requests
    WHERE ${where}
    GROUP BY 1, 2
    ORDER BY reqs DESC
    LIMIT 100`,
    [],
    params.signal,
  );

  const by_asn = byAsnRows.map((row) => {
    const rttMedUs = Number(row.rtt_med_us ?? 0);
    const rttBaseUs = Math.max(1, Number(row.rtt_base_us ?? rttMedUs ?? 1));
    const ploss = Number(row.avg_ploss ?? 0);
    const errPct = Number(row.error_pct ?? 0);
    return {
      asn: Number(row.asn ?? 0),
      as_name: String(row.as_name ?? ""),
      reqs: Number(row.reqs ?? 0),
      rtt_med_us: roundTo(rttMedUs, 1),
      rtt_med_ms: roundTo(rttMedUs / 1000, 2),
      avg_ploss: roundTo(ploss, 4),
      error_pct: roundTo(errPct, 2),
      health_score: computeNetworkHealthScore(
        rttMedUs,
        rttBaseUs,
        ploss,
        errPct,
      ),
    };
  });

  const metroRawRows = await engine.executeSql<MetroRow>(
    `SELECT
      COALESCE(geo_city, city, 'Unknown') AS city,
      COALESCE(geo_country, country, 'Unknown') AS country,
      COUNT(*)::BIGINT AS reqs,
      QUANTILE_CONT(COALESCE(client_rtt_us, client_tcpi_rtt), 0.5)::DOUBLE AS rtt_med_us,
      QUANTILE_CONT(COALESCE(client_rtt_us, client_tcpi_rtt), 0.1)::DOUBLE AS rtt_base_us,
      AVG(client_ploss)::DOUBLE AS avg_ploss,
      (COUNT(*) FILTER (WHERE status >= 500 AND status < 600) * 100.0 / NULLIF(COUNT(*), 0))::DOUBLE AS error_pct
    FROM requests
    WHERE ${where}
    GROUP BY 1, 2
    ORDER BY reqs DESC
    LIMIT 400`,
    [],
    params.signal,
  );

  const scoredMetros: MetroRow[] = metroRawRows.map((row) => {
    const rttMedUs = Number(row.rtt_med_us ?? 0);
    const rttBaseUs = Math.max(
      1,
      Number((row as Record<string, unknown>).rtt_base_us ?? rttMedUs ?? 1),
    );
    const ploss = Number(row.avg_ploss ?? 0);
    const errPct = Number(row.error_pct ?? 0);
    const reqs = Number(row.reqs ?? 0);
    return {
      city: String(row.city ?? "Unknown"),
      country: String(row.country ?? "Unknown"),
      reqs,
      total_reqs: reqs,
      rtt_med_us: roundTo(rttMedUs, 1),
      rtt_med_ms: roundTo(rttMedUs / 1000, 2),
      avg_ploss: roundTo(ploss, 4),
      avg_ploss_pct: roundTo(ploss <= 1 ? ploss * 100 : ploss, 2),
      error_pct: roundTo(errPct, 2),
      health_score: computeNetworkHealthScore(
        rttMedUs,
        rttBaseUs,
        ploss,
        errPct,
      ),
    };
  });

  const metro_leaderboard = deduplicateMetroLeaderboard(scoredMetros);
  const popHealth = await queryPopHealth(params);

  return {
    summary: {
      total_requests: totalRequests,
      avg_rtt_ms: roundTo(Number(s.avg_rtt_us ?? 0) / 1000, 2),
      median_rtt_ms: roundTo(medRttUs / 1000, 2),
      p95_rtt_ms: roundTo(Number(s.p95_rtt_us ?? 0) / 1000, 2),
      avg_ploss_pct: roundTo(avgPloss <= 1 ? avgPloss * 100 : avgPloss, 2),
      health_score: healthScore,
    },
    timeseries: [] as Record<string, unknown>[],
    by_country,
    by_asn,
    by_pop: popHealth.by_pop,
    metro_leaderboard,
    missing_tcp_fields: missingTcpFields,
  };
}

export async function queryNetworkQuality(params: NetworkQueryParams = {}) {
  const engine = params.engine ?? getDuckDBEngine();
  const missingTcpFields = await detectMissingTcpFields(engine);
  const where = buildWhereClause(params);

  const summaryRows = await engine.executeSql<{
    total_requests: number;
    avg_cwnd: number | null;
    avg_rtt_us: number | null;
    avg_rttvar_us: number | null;
    retrans_rate_pct: number | null;
    avg_throughput_mbps: number | null;
  }>(
    `SELECT
      COUNT(*)::BIGINT AS total_requests,
      AVG(client_cwnd)::DOUBLE AS avg_cwnd,
      AVG(COALESCE(client_rtt_us, client_tcpi_rtt))::DOUBLE AS avg_rtt_us,
      AVG(client_tcpi_rttvar)::DOUBLE AS avg_rttvar_us,
      (COUNT(*) FILTER (WHERE COALESCE(client_tcpi_delta_retrans, 0) > 0) * 100.0 / NULLIF(COUNT(*), 0))::DOUBLE AS retrans_rate_pct,
      AVG(CASE WHEN COALESCE(time_elapsed_usec, ttfb_ms * 1000) > 0 THEN (COALESCE(resp_body_size, resp_bytes, 0) * 8.0) / (COALESCE(time_elapsed_usec, ttfb_ms * 1000) / 1000.0) ELSE NULL END)::DOUBLE AS avg_throughput_mbps
    FROM requests
    WHERE ${where}`,
    [],
    params.signal,
  );

  const totalRequests = Number(summaryRows[0]?.total_requests ?? 0);
  if (totalRequests === 0) {
    return emptyNetworkQuality(missingTcpFields);
  }

  const byRegionRows = await engine.executeSql<Record<string, unknown>>(
    `SELECT
      COALESCE(geo_country, country, 'Unknown') AS region,
      COUNT(*)::BIGINT AS requests,
      AVG(COALESCE(client_rtt_us, client_tcpi_rtt))::DOUBLE AS avg_rtt_us,
      AVG(client_tcpi_rttvar)::DOUBLE AS avg_rttvar_us,
      AVG(client_ploss)::DOUBLE AS avg_ploss,
      AVG(client_cwnd)::DOUBLE AS avg_cwnd
    FROM requests
    WHERE ${where}
    GROUP BY 1
    ORDER BY requests DESC
    LIMIT 100`,
    [],
    params.signal,
  );

  const by_region = byRegionRows.map((r) => ({
    region: String(r.region ?? "Unknown"),
    requests: Number(r.requests ?? 0),
    avg_rtt_ms: roundTo(Number(r.avg_rtt_us ?? 0) / 1000, 2),
    avg_rttvar_ms: roundTo(Number(r.avg_rttvar_us ?? 0) / 1000, 2),
    avg_ploss_pct: roundTo(Number(r.avg_ploss ?? 0) * 100, 2),
    avg_cwnd: roundTo(Number(r.avg_cwnd ?? 0), 1),
  }));

  const byAsnRows = await engine.executeSql<Record<string, unknown>>(
    `SELECT
      COALESCE(client_asn, asn, 0)::BIGINT AS asn,
      COALESCE(as_name, 'AS' || CAST(COALESCE(client_asn, asn, 0) AS VARCHAR)) AS as_name,
      COUNT(*)::BIGINT AS requests,
      AVG(COALESCE(client_rtt_us, client_tcpi_rtt))::DOUBLE AS avg_rtt_us,
      AVG(client_tcpi_rttvar)::DOUBLE AS avg_rttvar_us,
      AVG(client_ploss)::DOUBLE AS avg_ploss,
      AVG(client_cwnd)::DOUBLE AS avg_cwnd
    FROM requests
    WHERE ${where}
    GROUP BY 1, 2
    ORDER BY requests DESC
    LIMIT 100`,
    [],
    params.signal,
  );

  const by_asn = byAsnRows.map((r) => ({
    asn: Number(r.asn ?? 0),
    as_name: String(r.as_name ?? ""),
    requests: Number(r.requests ?? 0),
    avg_rtt_ms: roundTo(Number(r.avg_rtt_us ?? 0) / 1000, 2),
    avg_rttvar_ms: roundTo(Number(r.avg_rttvar_us ?? 0) / 1000, 2),
    avg_ploss_pct: roundTo(Number(r.avg_ploss ?? 0) * 100, 2),
    avg_cwnd: roundTo(Number(r.avg_cwnd ?? 0), 1),
  }));

  const scatter = by_asn.slice(0, 50).map((r) => ({
    asn: r.asn,
    as_name: r.as_name,
    requests: r.requests,
    rtt_ms: r.avg_rtt_ms,
    ploss_pct: r.avg_ploss_pct,
    cwnd: r.avg_cwnd,
  }));

  const protoRows = await engine.executeSql<{
    proto: string | null;
    requests: number;
    avg_rtt_us: number | null;
  }>(
    `SELECT
      COALESCE(protocol, 'Other') AS proto,
      COUNT(*)::BIGINT AS requests,
      AVG(COALESCE(client_rtt_us, client_tcpi_rtt))::DOUBLE AS avg_rtt_us
    FROM requests
    WHERE ${where}
    GROUP BY 1`,
    [],
    params.signal,
  );

  const protoMap = new Map<
    string,
    { requests: number; rttWeightedUs: number }
  >();
  for (const b of PROTOCOL_BUCKETS) {
    protoMap.set(b, { requests: 0, rttWeightedUs: 0 });
  }
  for (const row of protoRows) {
    const bucket = normalizeProtocolBucket(row.proto);
    const cur = protoMap.get(bucket)!;
    const reqs = Number(row.requests ?? 0);
    const rtt = Number(row.avg_rtt_us ?? 0);
    cur.requests += reqs;
    cur.rttWeightedUs += rtt * reqs;
  }

  const protocol_distribution = PROTOCOL_BUCKETS.map((protocol) => {
    const entry = protoMap.get(protocol)!;
    return {
      protocol,
      requests: entry.requests,
      pct:
        totalRequests > 0
          ? roundTo((entry.requests * 100) / totalRequests, 2)
          : 0,
      avg_rtt_ms:
        entry.requests > 0
          ? roundTo(entry.rttWeightedUs / entry.requests / 1000, 2)
          : 0,
    };
  });

  const s = summaryRows[0];
  return {
    summary: {
      total_requests: totalRequests,
      avg_throughput_mbps: roundTo(Number(s.avg_throughput_mbps ?? 0), 2),
      avg_cwnd: roundTo(Number(s.avg_cwnd ?? 0), 1),
      avg_rtt_ms: roundTo(Number(s.avg_rtt_us ?? 0) / 1000, 2),
      avg_rttvar_ms: roundTo(Number(s.avg_rttvar_us ?? 0) / 1000, 2),
      retrans_rate_pct: roundTo(Number(s.retrans_rate_pct ?? 0), 2),
    },
    scatter,
    by_region,
    by_asn,
    protocol_distribution,
    missing_tcp_fields: missingTcpFields,
  };
}

export async function queryPopHealth(params: NetworkQueryParams = {}) {
  const engine = params.engine ?? getDuckDBEngine();
  const where = buildWhereClause(params);

  const rows = await engine.executeSql<Record<string, unknown>>(
    `SELECT
      COALESCE(fastly_pop, pop, 'Unknown') AS pop,
      COUNT(*)::BIGINT AS requests,
      QUANTILE_CONT(COALESCE(client_rtt_us, client_tcpi_rtt, ttfb_ms * 1000), 0.5)::DOUBLE AS p50_us,
      QUANTILE_CONT(COALESCE(client_rtt_us, client_tcpi_rtt, ttfb_ms * 1000), 0.95)::DOUBLE AS p95_us,
      QUANTILE_CONT(COALESCE(client_rtt_us, client_tcpi_rtt, ttfb_ms * 1000), 0.1)::DOUBLE AS baseline_p50_us,
      AVG(client_ploss)::DOUBLE AS avg_ploss,
      (COUNT(*) FILTER (WHERE status >= 500 AND status < 600) * 100.0 / NULLIF(COUNT(*), 0))::DOUBLE AS error_pct
    FROM requests
    WHERE ${where}
    GROUP BY 1
    ORDER BY requests DESC
    LIMIT 200`,
    [],
    params.signal,
  );

  if (rows.length === 0) {
    return emptyPopHealth();
  }

  let elevatedCount = 0;
  const pops = rows.map((r) => {
    const p50Ms = Number(r.p50_us ?? 0) / 1000;
    const p95Ms = Number(r.p95_us ?? 0) / 1000;
    const baselineMs = Math.max(0.1, Number(r.baseline_p50_us ?? 0) / 1000);
    const isElevated = baselineMs > 0 && p50Ms > baselineMs * 1.5;
    if (isElevated) elevatedCount++;
    return {
      pop: String(r.pop ?? "Unknown"),
      requests: Number(r.requests ?? 0),
      p50_ms: roundTo(p50Ms, 2),
      p95_ms: roundTo(p95Ms, 2),
      baseline_p50_ms: roundTo(baselineMs, 2),
      avg_ploss_pct: roundTo(Number(r.avg_ploss ?? 0) * 100, 2),
      error_pct: roundTo(Number(r.error_pct ?? 0), 2),
      is_elevated: isElevated,
    };
  });

  return {
    pops,
    by_pop: pops,
    elevated_count: elevatedCount,
    total_pops: pops.length,
  };
}

export async function queryNetworkBundle(params: NetworkQueryParams = {}) {
  const [network_health, network_quality, pop_health] = await Promise.all([
    queryNetworkHealth(params),
    queryNetworkQuality(params),
    queryPopHealth(params),
  ]);
  return {
    network_health,
    network_quality,
    pop_health,
    missing_tcp_fields: network_health.missing_tcp_fields,
  };
}

export async function handleNetworkRequest(
  path: string,
  params: NetworkQueryParams = {},
) {
  if (path.includes("/api/network/bundle")) {
    return queryNetworkBundle(params);
  }
  if (path.includes("/api/network-health")) {
    return queryNetworkHealth(params);
  }
  if (path.includes("/api/network-quality")) {
    return queryNetworkQuality(params);
  }
  if (path.includes("/api/network/pop-health")) {
    return queryPopHealth(params);
  }
  return queryNetworkBundle(params);
}
