import type { DuckDBReadContext } from "./dashboard/duckdb";
import { ensureCloudRange } from "./range-loader";
import { buildScoringSourceSql } from "./sessions";

export interface NormalizedRoute {
  path: string;
  category: string;
}

export interface ScoringMatrixJson {
  version: string;
  vocab_size: number;
  session_count: number;
  transition_count: number;
  counts: Record<string, Record<string, number>>;
  row_totals: Record<string, number>;
  categories: Record<string, string>;
  anchors: string[];
}

export interface MatrixTrainStats {
  sessions_in: number;
  sessions_kept: number;
  sessions_dropped_short: number;
  sessions_dropped_fast: number;
  transitions: number;
  routes_seen: number;
}

export interface LabeledSessionTrace {
  edge_sid: string;
  label: "good" | "bad" | "neutral";
  urls: string[];
}

const NUMERIC_ID = /^\d+$/;
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const HEX_HASH = /^[0-9a-fA-F]{24,}$/;
const PREFIXED_ID = /^[A-Z]{2,5}[-_][A-Za-z0-9_-]+$/;
const LONG_OPAQUE = /^[A-Za-z0-9_-]{20,}$/;

const CATEGORY_MAP: Record<string, string> = {
  "": "home",
  api: "api",
  graphql: "api",
  products: "product",
  product: "product",
  items: "product",
  p: "product",
  categories: "browse",
  category: "browse",
  search: "browse",
  browse: "browse",
  cart: "cart",
  basket: "cart",
  checkout: "checkout",
  pay: "checkout",
  order: "checkout",
  orders: "checkout",
  account: "account",
  user: "account",
  users: "account",
  profile: "account",
  settings: "account",
  auth: "auth",
  login: "auth",
  signin: "auth",
  signup: "auth",
  register: "auth",
  logout: "auth",
  admin: "admin",
  static: "asset",
  assets: "asset",
  blog: "content",
  news: "content",
  about: "content",
  help: "content",
  support: "content",
  privacy: "content",
  terms: "content",
  faq: "content",
};

function asciiLower(s: string): string {
  return s.replace(/[A-Z]/g, (c) => String.fromCharCode(c.charCodeAt(0) + 32));
}

function stripQuery(url: string): string {
  let clean = url.replace(/[\t\n\r]/g, "");
  while (clean.startsWith("//")) clean = clean.slice(1);
  if (/^https?:\/\//i.test(clean)) {
    try {
      return new URL(clean).pathname || "/";
    } catch {
      return "/";
    }
  }
  const qIdx = clean.indexOf("?");
  const hIdx = clean.indexOf("#");
  const cut = qIdx === -1 ? hIdx : hIdx === -1 ? qIdx : Math.min(qIdx, hIdx);
  const path = cut === -1 ? clean : clean.slice(0, cut);
  return path || "/";
}

function collapseSegment(seg: string): string {
  if (
    NUMERIC_ID.test(seg) ||
    UUID_RE.test(seg) ||
    HEX_HASH.test(seg) ||
    PREFIXED_ID.test(seg) ||
    LONG_OPAQUE.test(seg)
  ) {
    return "*";
  }
  return asciiLower(seg);
}

export function normalizeScoringRoute(url: string): NormalizedRoute {
  const rawPath = stripQuery(url);
  const decoded = (() => {
    try {
      return decodeURIComponent(rawPath);
    } catch {
      return rawPath;
    }
  })();
  const parts = decoded.split("/").filter((p) => p.length > 0 && p !== ".");
  const out: string[] = [];
  for (const part of parts) {
    if (part === "..") {
      out.pop();
      continue;
    }
    out.push(collapseSegment(part));
  }
  const path = out.length === 0 ? "/" : `/${out.join("/")}`;
  const first = out[0] ?? "";
  const category = CATEGORY_MAP[first] ?? "other";
  return { path, category };
}

function defaultMatrixVersion(nowMs = Date.now()): string {
  const d = new Date(nowMs);
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}-${pad(d.getUTCHours())}${pad(d.getUTCMinutes())}${pad(d.getUTCSeconds())}`;
}

export function scoreTransition(
  matrix: ScoringMatrixJson,
  prevPath: string,
  curPath: string,
): number {
  const rowTotal = matrix.row_totals[prevPath] ?? 0;
  if (rowTotal <= 0) return 0;
  const cnt = matrix.counts[prevPath]?.[curPath] ?? 0;
  return (cnt + 0.01) / (rowTotal + 0.01 * Math.max(1, matrix.vocab_size));
}

export function scoreSessionSurprise(
  matrix: ScoringMatrixJson,
  urls: string[],
): number {
  if (urls.length < 2 || matrix.vocab_size <= 0) return 0;
  let totalSurprisal = 0;
  let steps = 0;
  let prev = normalizeScoringRoute(urls[0]).path;
  for (let i = 1; i < urls.length; i++) {
    const cur = normalizeScoringRoute(urls[i]).path;
    const p = scoreTransition(matrix, prev, cur);
    totalSurprisal += -Math.log2(Math.max(1e-6, p));
    steps += 1;
    prev = cur;
  }
  return steps > 0 ? totalSurprisal / steps : 0;
}

export function computeRocAuc(
  positives: number[],
  negatives: number[],
): number | null {
  if (positives.length === 0 || negatives.length === 0) return null;
  let wins = 0;
  for (const p of positives) {
    for (const n of negatives) {
      if (p > n) wins += 1;
      else if (p === n) wins += 0.5;
    }
  }
  return wins / (positives.length * negatives.length);
}

export function evaluateMatrixAuc(
  matrix: ScoringMatrixJson,
  labeledSessions: LabeledSessionTrace[],
): number | null {
  const goodScores: number[] = [];
  const badScores: number[] = [];
  for (const sess of labeledSessions) {
    if (sess.label === "good") {
      goodScores.push(scoreSessionSurprise(matrix, sess.urls));
    } else if (sess.label === "bad") {
      badScores.push(scoreSessionSurprise(matrix, sess.urls));
    }
  }
  if (goodScores.length < 5 || badScores.length < 5) return null;
  return computeRocAuc(badScores, goodScores);
}

export async function trainCloudScoringMatrix(
  read: DuckDBReadContext,
  options: {
    sinceDays?: number;
    endMs?: number;
    minEvents?: number;
    minMeanDwellS?: number;
    version?: string;
    signal?: AbortSignal;
  } = {},
): Promise<{
  matrix: ScoringMatrixJson;
  stats: MatrixTrainStats;
  sessionUrlsBySid: Map<string, string[]>;
}> {
  const sinceDays = Math.min(90, Math.max(1, Math.floor(options.sinceDays ?? 7)));
  const endMs = options.endMs ?? Date.now();
  const startMs = endMs - sinceDays * 86_400_000;
  const minEvents = options.minEvents ?? 3;
  const minMeanDwellS = options.minMeanDwellS ?? 1.0;

  const cols = await read.getPresentColumns();
  const source = buildScoringSourceSql("requests", cols);
  const hasEdgeSid = cols.has("edge_sid");
  const rows = await read.executeSql(
    `SELECT ts_ms, ip, ua, url${hasEdgeSid ? ", edge_sid" : ""}
     FROM ${source}
     WHERE ts_ms >= ${startMs} AND ts_ms <= ${endMs}
       AND ip IS NOT NULL AND url IS NOT NULL
     ORDER BY ip, ua, ts_ms ASC
     LIMIT 250000`,
    [],
    options.signal,
  );

  const sessions = new Map<string, Array<{ tsMs: number; url: string; edgeSid?: string }>>();
  for (const row of rows) {
    const ip = String(row.ip ?? "");
    const ua = String(row.ua ?? "");
    const url = String(row.url ?? "/");
    const tsMs = Number(row.ts_ms ?? 0);
    const key = `${ip}\0${ua}`;
    let list = sessions.get(key);
    if (!list) {
      list = [];
      sessions.set(key, list);
    }
    list.push({
      tsMs,
      url,
      edgeSid: hasEdgeSid && row.edge_sid ? String(row.edge_sid) : undefined,
    });
  }

  const counts: Record<string, Record<string, number>> = {};
  const rowTotals: Record<string, number> = {};
  const categories: Record<string, string> = {};
  const vocab = new Set<string>();
  const sessionUrlsBySid = new Map<string, string[]>();

  let sessionsIn = 0;
  let sessionsKept = 0;
  let sessionsDroppedShort = 0;
  let sessionsDroppedFast = 0;
  let transitionCount = 0;

  for (const events of sessions.values()) {
    sessionsIn += 1;
    const sid = events.find((e) => e.edgeSid)?.edgeSid;
    if (sid) {
      sessionUrlsBySid.set(
        sid,
        events.map((e) => e.url),
      );
    }
    if (events.length < minEvents) {
      sessionsDroppedShort += 1;
      continue;
    }
    const spanS = (events[events.length - 1].tsMs - events[0].tsMs) / 1000;
    const meanDwell = spanS / Math.max(1, events.length - 1);
    if (meanDwell < minMeanDwellS) {
      sessionsDroppedFast += 1;
      continue;
    }

    sessionsKept += 1;
    let prev: NormalizedRoute | null = null;
    for (const ev of events) {
      const curr = normalizeScoringRoute(ev.url);
      vocab.add(curr.path);
      categories[curr.path] = curr.category;
      if (prev !== null) {
        counts[prev.path] ??= {};
        counts[prev.path][curr.path] = (counts[prev.path][curr.path] ?? 0) + 1;
        rowTotals[prev.path] = (rowTotals[prev.path] ?? 0) + 1;
        transitionCount += 1;
      }
      prev = curr;
    }
  }

  const matrix: ScoringMatrixJson = {
    version: options.version || defaultMatrixVersion(),
    vocab_size: vocab.size,
    session_count: sessionsKept,
    transition_count: transitionCount,
    counts,
    row_totals: rowTotals,
    categories,
    anchors: [],
  };

  return {
    matrix,
    stats: {
      sessions_in: sessionsIn,
      sessions_kept: sessionsKept,
      sessions_dropped_short: sessionsDroppedShort,
      sessions_dropped_fast: sessionsDroppedFast,
      transitions: transitionCount,
      routes_seen: vocab.size,
    },
    sessionUrlsBySid,
  };
}

export async function retrainCloudScoring(
  serviceId: string,
  options: {
    sinceDays?: number;
    endMs?: number;
    version?: string;
    readContext?: DuckDBReadContext;
    signal?: AbortSignal;
  } = {},
): Promise<{
  ok: boolean;
  version: string;
  retrained_at?: string;
  kv_pushed?: boolean;
  auc_against_labels: number | null;
  n_good?: number;
  n_bad?: number;
  stats: MatrixTrainStats;
}> {
  const sinceDays = Math.min(90, Math.max(1, Math.floor(options.sinceDays ?? 7)));
  const endMs = options.endMs ?? Date.now();
  const startMs = endMs - sinceDays * 86_400_000;
  const controller = new AbortController();
  const read =
    options.readContext ??
    (await ensureCloudRange(
      new Date(startMs).toISOString(),
      new Date(endMs).toISOString(),
      options.signal ?? controller.signal,
      ["request"],
      { serviceId },
    ));

  const { matrix, stats, sessionUrlsBySid } = await trainCloudScoringMatrix(read, {
    sinceDays,
    endMs,
    version: options.version,
    signal: options.signal,
  });

  let aucAgainstLabels: number | null = null;
  let nGood = 0;
  let nBad = 0;
  try {
    const labelsResp = await fetch(
      `/api/services/${encodeURIComponent(serviceId)}/scoring/labels`,
      { signal: options.signal },
    );
    if (labelsResp.ok) {
      const payload = (await labelsResp.json()) as {
        labels?: Array<{ sid?: string; edge_sid?: string; label: "good" | "bad" | "neutral" }>;
      };
      const traces: LabeledSessionTrace[] = [];
      for (const item of payload.labels ?? []) {
        const sid = item.sid ?? item.edge_sid ?? "";
        if (!sid) continue;
        if (item.label === "good") nGood += 1;
        else if (item.label === "bad") nBad += 1;
        const urls = sessionUrlsBySid.get(sid);
        if (urls && urls.length >= 2) {
          traces.push({ edge_sid: sid, label: item.label, urls });
        }
      }
      aucAgainstLabels = evaluateMatrixAuc(matrix, traces);
    }
  } catch {
    // Evaluation against labels is best-effort; do not block retrain if labels are empty.
  }

  const res = await fetch(
    `/api/services/${encodeURIComponent(serviceId)}/scoring/retrain`,
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        matrix,
        stats,
        auc_against_labels: aucAgainstLabels,
      }),
      signal: options.signal,
    },
  );
  const body = (await res.json()) as {
    ok?: boolean;
    version?: string;
    retrained_at?: string;
    kv_pushed?: boolean;
    detail?: { error?: string };
    error?: string;
  };
  if (!res.ok) {
    throw new Error(body.detail?.error || body.error || `Retrain failed (${res.status})`);
  }
  return {
    ok: body.ok ?? true,
    version: body.version ?? matrix.version,
    retrained_at: body.retrained_at,
    kv_pushed: body.kv_pushed,
    auc_against_labels: aucAgainstLabels,
    n_good: nGood,
    n_bad: nBad,
    stats,
  };
}
