import { parquetMetadata } from "hyparquet";
import { fnv1a64 } from "../parquet-key";
import type { FileRollupData, MinuteBucket, Scalar } from "./engine";
import { Hll } from "./hll";
import type { ManifestFile } from "./manifest";

export type CardTint = { bg: string; border: string; dot: string };

export type CardCategory = {
  id: string;
  label: string;
  cardIds: string[];
  tint: CardTint;
};

// v3 CARD_CATEGORIES narrowed to Top-N fields in app/src/logging/catalog.json:
// NON_TOP_N numeric fields, virtual bot/WAF fields and cookie_session are
// omitted, so the Origin and QUIC categories have no Top-N cards.
export const CARD_CATEGORIES: CardCategory[] = [
  {
    id: "request",
    label: "Request",
    cardIds: [
      "ip",
      "asn",
      "host",
      "url",
      "method",
      "status",
      "cache",
      "proto",
      "ua",
      "referer",
      "resp_header_content_encoding",
    ],
    tint: {
      bg: "bg-blue-50/60 dark:bg-blue-950/40",
      border: "border-blue-200/70 dark:border-blue-900/60",
      dot: "bg-blue-500",
    },
  },
  {
    id: "cache",
    label: "Cache",
    cardIds: ["ttl", "age", "hits", "digest"],
    tint: {
      bg: "bg-amber-50/60 dark:bg-amber-950/40",
      border: "border-amber-200/70 dark:border-amber-900/60",
      dot: "bg-amber-500",
    },
  },
  {
    id: "geo",
    label: "Geography",
    cardIds: ["city", "region", "country", "metro"],
    tint: {
      bg: "bg-emerald-50/60 dark:bg-emerald-950/40",
      border: "border-emerald-200/70 dark:border-emerald-900/60",
      dot: "bg-emerald-500",
    },
  },
  {
    id: "network",
    label: "Network & Connection",
    cardIds: ["transport", "c_speed", "c_type"],
    tint: {
      bg: "bg-cyan-50/60 dark:bg-cyan-950/40",
      border: "border-cyan-200/70 dark:border-cyan-900/60",
      dot: "bg-cyan-500",
    },
  },
  {
    id: "edge",
    label: "Edge Infrastructure",
    cardIds: [
      "pop",
      "backend",
      "edge",
      "server_region",
      "tls",
      "is_ipv6",
      "conn_requests",
    ],
    tint: {
      bg: "bg-violet-50/60 dark:bg-violet-950/40",
      border: "border-violet-200/70 dark:border-violet-900/60",
      dot: "bg-violet-500",
    },
  },
  {
    id: "security",
    label: "Security",
    cardIds: [
      "waf",
      "waf_resp",
      "waf_ms",
      "p_type",
      "p_desc",
      "ja3",
      "ja4",
      "tls_ciphers_sha",
    ],
    tint: {
      bg: "bg-rose-50/60 dark:bg-rose-950/40",
      border: "border-rose-200/70 dark:border-rose-900/60",
      dot: "bg-rose-500",
    },
  },
  {
    id: "io",
    label: "Image Optimization",
    cardIds: ["io_input_format", "io_output_format"],
    tint: {
      bg: "bg-fuchsia-50/60 dark:bg-fuchsia-950/40",
      border: "border-fuchsia-200/70 dark:border-fuchsia-900/60",
      dot: "bg-fuchsia-500",
    },
  },
];

export const CARD_LABELS: Record<string, string> = {
  ip: "Client IP",
  asn: "ASN",
  host: "Host",
  url: "URL",
  method: "HTTP Method",
  status: "Response Status",
  cache: "Cache State",
  proto: "HTTP Version",
  ua: "User-Agent",
  referer: "Referer",
  resp_header_content_encoding: "Response Content-Encoding",
  ttl: "Object TTL",
  age: "Object Age",
  hits: "Object Hit Count",
  digest: "Content Digest",
  city: "City",
  region: "Region",
  country: "Country",
  metro: "Metro Code",
  transport: "Transport Protocol",
  c_speed: "Connection Speed Class",
  c_type: "Connection Type",
  pop: "Edge PoP",
  backend: "Backend",
  edge: "Edge Hit",
  server_region: "Server Region",
  tls: "TLS Version",
  is_ipv6: "IPv6",
  conn_requests: "Conn. Request Count",
  waf: "WAF Executed",
  waf_resp: "WAF Agent Response",
  waf_ms: "WAF Latency (ms)",
  p_type: "Proxy Type",
  p_desc: "Proxy Description",
  ja3: "JA3 Fingerprint",
  ja4: "JA4 Fingerprint",
  tls_ciphers_sha: "TLS Cipher Suite SHA",
  io_input_format: "IO Input Format",
  io_output_format: "IO Output Format",
};

export const CARD_FIELDS = CARD_CATEGORIES.flatMap((c) => c.cardIds);
export type CardField = (typeof CARD_FIELDS)[number];

/** A custom log field marked "Show in dashboard" (session `custom_dashboard_cards`). */
export type CustomCard = {
  id: string;
  label: string;
  type: "string" | "number" | "boolean";
};

// v3 DashboardWidgets CUSTOM_TINT.
export const CUSTOM_TINT: CardTint = {
  bg: "bg-slate-50/60 dark:bg-slate-900/30",
  border: "border-slate-200/60 dark:border-slate-800/50",
  dot: "bg-slate-400",
};

const SYSTEM_PREFIXES = ["cmcd_", "edge_", "rum_"];

/**
 * v3 useDashboardCardDefinitions: system-managed CMCD, edge and RUM fields
 * and the request id never get a card, nor do duplicates of a built-in card.
 */
export function customCards(cards: readonly CustomCard[] | undefined) {
  const builtIn = new Set<string>(CARD_FIELDS);
  const seen = new Set<string>();
  return (cards ?? []).filter((card) => {
    if (
      builtIn.has(card.id) ||
      seen.has(card.id) ||
      card.id === "fastly_req_id" ||
      SYSTEM_PREFIXES.some((prefix) => card.id.startsWith(prefix))
    ) {
      return false;
    }
    seen.add(card.id);
    return true;
  });
}

/** Stable key of the custom fields a run aggregates, in request order. */
export function customFieldsKey(
  cards: readonly Pick<CustomCard, "id" | "type">[],
): string {
  return cards.map((card) => `${card.id}:${card.type}`).join(",");
}

/** `flp_summary` written by app/src/ingest/summary.rs `SUMMARY_VERSION`. */
export const SUMMARY_VERSION = "2";
const MINUTE_MS = 60_000;

const BOOLEAN_FIELDS = new Set(["edge", "is_ipv6", "waf"]);
const NUMBER_FIELDS = new Set([
  "asn",
  "status",
  "ttl",
  "age",
  "hits",
  "metro",
  "conn_requests",
  "waf_resp",
  "waf_ms",
]);

/** A custom field a run aggregates and its value type. */
export type CustomField = Pick<CustomCard, "id" | "type">;

/** Summaries store every value as text; this restores the raw column's type. */
export function summaryValue(
  field: string,
  value: string,
  type?: CustomField["type"],
): Scalar | null {
  if (type === "boolean" || (!type && BOOLEAN_FIELDS.has(field))) {
    return value === "true";
  }
  if (type === "number" || (!type && NUMBER_FIELDS.has(field))) {
    const n = Number(value);
    return value === "" || Number.isNaN(n) ? null : n;
  }
  return value;
}

/** FNV-1a 64 over each name followed by `\n` (app/src/ingest/bundle.rs `fingerprint`). */
export function fingerprint(names: readonly string[]): bigint {
  return fnv1a64(names);
}

/** `c{count}-n{rows}-h{fp}.parquet` of the sorted source names. */
function covering(names: string[], rows: number): string {
  const sorted = [...names].sort();
  return `c${sorted.length}-n${rows}-h${fingerprint(sorted).toString(16).padStart(16, "0")}.parquet`;
}

const RAW_WINDOW =
  /(?:^|\/)processed\/request\/(year=\d+\/month=\d+\/day=\d+\/)hour=(\d{2})\/window=(\d+-\d+-[^/]*-n(\d+))\.parquet$/;
const RAW_COMPACT =
  /(?:^|\/)processed\/request\/(year=\d+\/month=\d+\/day=\d+\/)hour=(\d{2})\/(?:catalog1\/[0-9a-f]{64}\/)?compact=(\d+-\d+-c(\d+)-n(\d+)(?:-[0-9a-f]+)?)\.parquet$/;
const WINDOW_SUMMARY =
  /(?:^|\/)processed\/summary\/request\/(year=\d+\/month=\d+\/day=\d+\/hour=\d{2}\/)summary=([^/]+)\.parquet$/;
const HOUR_BUNDLE =
  /(?:^|\/)processed\/summary_bundle\/request\/(?:sha256=[0-9a-f]{64}\/)?(year=\d+\/month=\d+\/day=\d+\/)hour_bundle=(\d{2})-(c(\d+)-n(\d+)-h[0-9a-f]{16}\.parquet)$/;
const DAY_BUNDLE =
  /(?:^|\/)processed\/summary_bundle\/request\/(?:sha256=[0-9a-f]{64}\/)?(year=\d+\/month=\d+\/day=\d+\/)day_bundle=(c(\d+)-n(\d+)-h[0-9a-f]{16}\.parquet)$/;

export type SummaryUse = { file: ManifestFile; covers: ManifestFile[] };

export type SummaryPlan = {
  summaries: SummaryUse[];
  /** Window files no summary answers exactly. */
  raw: ManifestFile[];
};

type RawWindow = {
  file: ManifestFile;
  day: string;
  hour: string;
  suffix: string;
  rows: number;
  sources: number;
  compacted: boolean;
};

/**
 * Picks the summaries that answer raw windows wholly inside [fromMs, toMs).
 * A window summary answers its own window; a bundle is used only when its name
 * proves it covers exactly the remaining windows of its hour or day, so a late,
 * missing or extra window sends that hour or day back to raw Parquet.
 */
export function planSummaries(
  rawFiles: readonly ManifestFile[],
  summaryFiles: readonly ManifestFile[],
  fromMs: number,
  toMs: number,
): SummaryPlan {
  const interior = new Map<string, RawWindow>();
  const rawIds = new Map<string, string>();
  const partialHours = new Set<string>();
  const partialDays = new Set<string>();
  for (const file of rawFiles) {
    const windowMatch = RAW_WINDOW.exec(file.key);
    const compactMatch = windowMatch ? null : RAW_COMPACT.exec(file.key);
    if (!windowMatch && !compactMatch) continue;
    const day = (windowMatch ?? compactMatch)![1];
    const hour = (windowMatch ?? compactMatch)![2];
    if (Date.parse(file.start) < fromMs || Date.parse(file.end) > toMs) {
      partialHours.add(`${day}hour=${hour}/`);
      partialDays.add(day);
      continue;
    }
    if (windowMatch) {
      const id = `${day}hour=${hour}/${windowMatch[3]}`;
      rawIds.set(file.key, id);
      interior.set(id, {
        file,
        day,
        hour,
        suffix: windowMatch[3],
        rows: Number(windowMatch[4]),
        sources: 1,
        compacted: false,
      });
    } else if (compactMatch) {
      const id = `${day}hour=${hour}/compact=${compactMatch[3]}`;
      rawIds.set(file.key, id);
      interior.set(id, {
        file,
        day,
        hour,
        suffix: compactMatch[3],
        rows: Number(compactMatch[5]),
        sources: Number(compactMatch[4]),
        compacted: true,
      });
    }
  }
  const claimed = new Set<string>();
  const summaries: SummaryUse[] = [];
  const byKind = (re: RegExp) =>
    summaryFiles.flatMap((file) => {
      const match = re.exec(file.key);
      return match ? [{ file, match }] : [];
    });
  const unclaimed = (pred: (w: RawWindow) => boolean) =>
    [...interior].filter(([id, w]) => !claimed.has(id) && pred(w));
  const claim = (file: ManifestFile, windows: [string, RawWindow][]) => {
    for (const [id] of windows) claimed.add(id);
    summaries.push({ file, covers: windows.map(([, w]) => w.file) });
  };
  const hourBundleName = (windows: RawWindow[]) =>
    covering(
      windows.map((w) => `summary=${w.suffix}.parquet`),
      windows.reduce((sum, w) => sum + w.rows, 0),
    );

  for (const { file, match } of byKind(WINDOW_SUMMARY)) {
    const id = `${match[1]}${match[2]}`;
    if (interior.has(id) && !claimed.has(id)) {
      claim(file, [[id, interior.get(id)!]]);
    }
  }
  for (const { file, match } of byKind(HOUR_BUNDLE)) {
    const windows = unclaimed((w) => w.day === match[1] && w.hour === match[2]);
    if (windows.length === 0) continue;
    const list = windows.map(([, w]) => w);
    const hasCompact = list.some((w) => w.compacted);
    const matchesHour = hasCompact
      ? !partialHours.has(`${match[1]}hour=${match[2]}/`) &&
        list.reduce((sum, w) => sum + w.sources, 0) <= Number(match[4]) &&
        list.reduce((sum, w) => sum + w.rows, 0) === Number(match[5])
      : hourBundleName(list) === match[3];
    if (matchesHour) {
      claim(file, windows);
    }
  }
  for (const { file, match } of byKind(DAY_BUNDLE)) {
    const windows = unclaimed((w) => w.day === match[1]);
    if (windows.length === 0) continue;
    const hours = new Map<string, RawWindow[]>();
    for (const [, w] of windows) {
      hours.set(w.hour, [...(hours.get(w.hour) ?? []), w]);
    }
    const list = windows.map(([, w]) => w);
    const rows = list.reduce((sum, w) => sum + w.rows, 0);
    const hasCompact = list.some((w) => w.compacted);
    const matchesDay = hasCompact
      ? !partialDays.has(match[1]) &&
        hours.size === Number(match[3]) &&
        rows === Number(match[4])
      : covering(
          [...hours].map(
            ([hour, hourList]) =>
              `hour_bundle=${hour}-${hourBundleName(hourList)}`,
          ),
          rows,
        ) === match[2];
    if (matchesDay) {
      claim(file, windows);
    }
  }
  return {
    summaries,
    raw: rawFiles.filter((file) => {
      const id = rawIds.get(file.key);
      return !id || !claimed.has(id);
    }),
  };
}

export type DecodedSummary = {
  rollup: FileRollupData;
  /** Card fields whose values were cut to the top K in some source. */
  truncated: string[];
  /** Distinct client IPs across the summary. */
  ipSketch: Hll | null;
};

const SUMMARY_COLUMNS = [
  "bundle",
  "bucket",
  "field",
  "value",
  "count",
  "truncated",
  "requests",
  "status_4xx",
  "status_5xx",
  "hits",
  "cache_total",
  "resp_bytes_sum",
];

const num = (value: unknown) =>
  typeof value === "bigint"
    ? Number(value)
    : typeof value === "number"
      ? value
      : 0;

function ms(value: unknown): number | null {
  if (value instanceof Date) return value.getTime();
  if (typeof value === "bigint") return Number(value);
  if (typeof value === "number") return value;
  return null;
}

/**
 * Decodes a window summary or bundle into a rollup for the card fields, or
 * returns null when it is another format version, reaches outside
 * [fromMs, toMs) or never summarized one of `custom` (it predates the field),
 * so the caller reads raw Parquet instead.
 */
export async function decodeSummary(
  bytes: ArrayBuffer,
  fromMs: number,
  toMs: number,
  custom: readonly CustomField[] = [],
): Promise<DecodedSummary | null> {
  const version = parquetMetadata(bytes).key_value_metadata?.find(
    (kv) => kv.key === "flp_summary",
  )?.value;
  if (version !== SUMMARY_VERSION) return null;
  const { readColumns } = await import("./parquet");
  const { rows, columns } = await readColumns(bytes, SUMMARY_COLUMNS);
  const binary = await readColumns(bytes, ["ip_sketch"], false);
  const col = (name: string) => columns.get(name) ?? [];
  const [kind, bucket, field, value, count, truncated] = [
    col("bundle"),
    col("bucket"),
    col("field"),
    col("value"),
    col("count"),
    col("truncated"),
  ];
  const [requests, s4xx, s5xx, hits, cacheTotal, respBytes, sketch] = [
    col("requests"),
    col("status_4xx"),
    col("status_5xx"),
    col("hits"),
    col("cache_total"),
    col("resp_bytes_sum"),
    binary.columns.get("ip_sketch") ?? [],
  ];
  const cards = new Set<string>(CARD_FIELDS);
  const types = new Map(custom.map((field) => [field.id, field.type]));
  for (const field of custom) cards.add(field.id);
  const summarized = new Set<string>();
  const buckets = new Map<number, MinuteBucket>();
  const counts = new Map<string, Map<Scalar, number>>();
  const nonNull: Record<string, number> = {};
  const cut = new Set<string>();
  let ipSketch: Hll | null = null;
  let total = 0;
  for (let i = 0; i < rows; i++) {
    switch (kind[i]) {
      case "time_series": {
        const t = ms(bucket[i]);
        if (t === null || t < fromMs || t + MINUTE_MS > toMs) return null;
        const b = buckets.get(t) ?? {
          t,
          requests: 0,
          bytes: 0,
          s4xx: 0,
          s5xx: 0,
          hits: 0,
          cacheRows: 0,
        };
        b.requests += num(requests[i]);
        b.bytes += num(respBytes[i]);
        b.s4xx += num(s4xx[i]);
        b.s5xx += num(s5xx[i]);
        b.hits += num(hits[i]);
        b.cacheRows += num(cacheTotal[i]);
        buckets.set(t, b);
        total += num(requests[i]);
        break;
      }
      case "overview": {
        const blob = sketch[i];
        const hll = blob instanceof Uint8Array ? Hll.fromBytes(blob) : null;
        if (hll) {
          if (ipSketch) ipSketch.merge(hll);
          else ipSketch = hll;
        }
        break;
      }
      case "field_totals": {
        const name = field[i] as string;
        if (!cards.has(name)) break;
        summarized.add(name);
        nonNull[name] = (nonNull[name] ?? 0) + num(count[i]);
        if (truncated[i] === true) cut.add(name);
        break;
      }
      case "all_fields": {
        const name = field[i] as string;
        if (!cards.has(name)) break;
        const typed = summaryValue(name, value[i] as string, types.get(name));
        if (typed === null) break;
        const map = counts.get(name) ?? new Map<Scalar, number>();
        map.set(typed, (map.get(typed) ?? 0) + num(count[i]));
        counts.set(name, map);
        break;
      }
    }
  }
  if (custom.some((field) => !summarized.has(field.id))) return null;
  const sorted = [...buckets.values()].sort((a, b) => a.t - b.t);
  return {
    rollup: {
      minTs: sorted.length ? sorted[0].t : Infinity,
      maxTs: sorted.length ? sorted[sorted.length - 1].t : -Infinity,
      totalRequests: total,
      hasBytes: true,
      buckets: sorted,
      counts: Object.fromEntries(
        [...counts].map(([name, map]) => [name, [...map]]),
      ),
      nonNull,
    },
    truncated: [...cut],
    ipSketch,
  };
}

export const readSummaryParquet = decodeSummary;
export const readSummaryBundleParquet = decodeSummary;
