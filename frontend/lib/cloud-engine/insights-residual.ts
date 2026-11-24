import { parquetMetadata, type SchemaElement } from "hyparquet";
import { Histogram } from "./dashboard/histogram";
import { readColumns } from "./dashboard/parquet";
import { emptyAgg, type Rollup, type RollupAgg, type RollupResidual } from "./dashboard/rollup-query";

const SERIES_STEP_MS = 300_000;

/** The `all_fields` counts /insights reads (`insights-rollup.ts`); `ip` bounds content discovery. */
export const INSIGHTS_RESIDUAL_FIELDS = ["country", "p_type", "method", "referer", "ua", "asn", "cache", "transport", "ip"] as const;

const KEYS = new Set([
  "dims/city",
  "series5/overall",
  ...INSIGHTS_RESIDUAL_FIELDS.map((field) => `all_fields/${field}`),
  "ins_url/url",
  "ins_ip/ip",
  "ins_lat/city",
  "ins_lat/pop",
  "ins_asn/asn",
  "urls/url",
]);

/** Keys `present` in every v3 summary that has the /insights groups. */
const PRESENT = ["dims/city", "series5/overall", "ins_url/url", "ins_ip/ip", "ins_lat/city", "ins_lat/pop", "ins_asn/asn", "urls/url"];

const COLUMNS = [
  "timestamp", "status", "cache", "url", "elapsed", "ip", "country", "geo_country", "city", "geo_city",
  "region", "pop", "asn", "p_type", "method", "referer", "ua", "transport",
];

/** summary.rs `AUTH_TOKENS`, matched ASCII case-insensitively. */
const AUTH_TOKENS = [
  "login", "signin", "sign-in", "log-in", "auth", "oauth", "openid", "token", "sso", "saml", "session",
  "account", "password", "passwd", "passwrd", "credential", "xmlrpc", "mfa", "otp", "2fa", "verify",
];

type Kind = "s" | "i" | "b" | "f" | "ts" | null;

/** The Arrow array summary.rs downcasts a column to, by its Parquet type; others it ignores. */
function kindOf(element: SchemaElement): Kind {
  const logical = element.logical_type;
  switch (element.type) {
    case "BYTE_ARRAY":
      return logical?.type === "STRING" || element.converted_type === "UTF8" ? "s" : null;
    case "INT64":
      if (logical?.type === "TIMESTAMP") return logical.unit === "MILLIS" ? "ts" : null;
      if (element.converted_type === "TIMESTAMP_MILLIS") return "ts";
      if (element.converted_type === "TIMESTAMP_MICROS") return null;
      if (logical?.type === "INTEGER" && !logical.isSigned) return null;
      return "i";
    case "BOOLEAN":
      return "b";
    case "DOUBLE":
      return "f";
    default:
      return null;
  }
}

const toNumber = (v: unknown): number | null =>
  typeof v === "bigint" ? Number(v) : typeof v === "number" ? v : v instanceof Date ? v.getTime() : null;

/** Rust's `f64` Display: shortest round-trip digits, never in exponent form. */
export function rustF64(v: number): string {
  const s = String(v);
  const exp = /^(-?)(\d)(?:\.(\d+))?e([+-]\d+)$/.exec(s);
  if (!exp) return s;
  const [, sign, lead, rest = "", power] = exp;
  const digits = lead + rest;
  const point = 1 + Number(power);
  if (point <= 0) return `${sign}0.${"0".repeat(-point)}${digits}`;
  return `${sign}${digits.padEnd(point, "0")}`;
}

/** summary.rs `FieldCol::for_each_key(i, false, ..)`'s one key, or null for none. */
function fieldKey(kind: Kind, v: unknown): string | null {
  if (v == null) return null;
  switch (kind) {
    case "s":
      return typeof v === "string" ? v : null;
    case "i":
      return typeof v === "bigint" || typeof v === "number" ? String(v) : null;
    case "b":
      return typeof v === "boolean" ? String(v) : null;
    case "f":
      return typeof v === "number" && Number.isFinite(v) ? rustF64(v === 0 ? 0 : v) : null;
    default:
      return null;
  }
}

const containsAuthToken = (url: string) => {
  const lower = url.replace(/[A-Z]+/g, (c) => c.toLowerCase());
  return AUTH_TOKENS.some((token) => lower.includes(token));
};

function aggOf(values: Map<string, RollupAgg>, key: string): RollupAgg {
  let agg = values.get(key);
  if (!agg) {
    agg = emptyAgg();
    values.set(key, agg);
  }
  return agg;
}

function addElapsed(agg: RollupAgg, elapsed: number | null) {
  if (elapsed === null) return;
  (agg.hists.elapsed ??= new Histogram()).add(elapsed);
  agg.elapsedCount += 1;
  agg.elapsedSum += elapsed;
}

/**
 * Rolls up raw rows into what the /insights rollup reads, as summary.rs
 * `write_v3`, `write_insights`, `write_insights_lat_asn` and `write_summary`
 * count them: `dims/city` requests and errors, the `series5` overall request
 * series, the `all_fields` counts of `INSIGHTS_RESIDUAL_FIELDS`, `ins_url`,
 * `ins_ip`, `ins_lat`, `ins_asn` and `urls/url` requests with their elapsed
 * histograms. Rows are never cut to a top K, so the residual is never truncated.
 */
export const INSIGHTS_RESIDUAL: RollupResidual = {
  id: "insights-residual-v2",
  columns: COLUMNS,
  maxBytes: 256 * 1024 * 1024,
  keys: KEYS,
  accumulate(fromMs, toMs) {
    const cities = new Map<string, RollupAgg>();
    const overall = new Map<number, RollupAgg>();
    const fields = new Map<string, Map<string, number>>(INSIGHTS_RESIDUAL_FIELDS.map((field) => [field, new Map()]));
    const insUrl = new Map<string, RollupAgg>();
    const insIp = new Map<string, RollupAgg>();
    const latCity = new Map<string, RollupAgg>();
    const latPop = new Map<string, RollupAgg>();
    const insAsn = new Map<string, RollupAgg>();
    const urls = new Map<string, RollupAgg>();
    return {
      async add(file) {
        const kinds = new Map<string, Kind>(
          parquetMetadata(file).schema.slice(1).map((element) => [element.name, kindOf(element)]),
        );
        if (kinds.get("timestamp") !== "ts") return;
        const { rows, columns } = await readColumns(file, COLUMNS.filter((name) => kinds.get(name)));
        const empty: ArrayLike<unknown> = { length: rows };
        const column = (name: string, kind: Kind) =>
          kinds.get(name) === kind ? (columns.get(name) ?? empty) : empty;
        const str = (name: string) => column(name, "s") as ArrayLike<string | null | undefined>;
        const int = (name: string) => column(name, "i");
        const ts = column("timestamp", "ts");
        const status = int("status");
        const elapsedCol = int("elapsed");
        const [cache, ip, plainCountry, plainCity, region, pop, pType] =
          ["cache", "ip", "country", "city", "region", "pop", "p_type"].map(str);
        const geoCountry = str("geo_country");
        const geoCity = str("geo_city");
        const urlKind = kinds.get("url") ?? null;
        const url = columns.get("url") ?? empty;
        const asnKind = kinds.get("asn") ?? null;
        const asn = columns.get("asn") ?? empty;
        const fieldCols = INSIGHTS_RESIDUAL_FIELDS.map((field) => ({
          counts: fields.get(field)!,
          kind: kinds.get(field) ?? null,
          values: columns.get(field) ?? empty,
        }));
        const nonEmpty = (v: string | null | undefined) => (v ? v : null);
        for (let i = 0; i < rows; i++) {
          const t = toNumber(ts[i]);
          if (t === null || t < fromMs || t >= toMs) continue;
          const s = toNumber(status[i]);
          const elapsed = toNumber(elapsedCol[i]);
          const c = cache[i] ?? null;

          const city = aggOf(cities, JSON.stringify([
            geoCountry[i] ?? plainCountry[i] ?? "Unknown",
            geoCity[i] ?? plainCity[i] ?? "Unknown",
          ]));
          city.requests += 1;
          if (s !== null && (s >= 400 || s === 0)) city.errors += 1;
          if (s !== null && s >= 500 && s <= 599) city.status5xx += 1;

          const step = Math.floor(t / SERIES_STEP_MS) * SERIES_STEP_MS;
          const bucket = overall.get(step) ?? emptyAgg();
          overall.set(step, bucket);
          bucket.requests += 1;
          if (s !== null && s >= 500 && s <= 599) bucket.status5xx += 1;

          for (const { counts, kind, values } of fieldCols) {
            const key = fieldKey(kind, values[i]);
            if (key !== null) counts.set(key, (counts.get(key) ?? 0) + 1);
          }

          const urlKey = fieldKey(urlKind, url[i]);
          if (urlKey !== null) {
            const u = aggOf(insUrl, urlKey);
            u.requests += 1;
            if (s !== null && s >= 500) u.status5xx += 1;
            if (c?.startsWith("HIT")) u.hitRequests += 1;
            if (c?.startsWith("MISS")) u.missRequests += 1;
            if (c?.startsWith("PASS")) u.passRequests += 1;
            const timed = aggOf(urls, urlKey);
            timed.requests += 1;
            addElapsed(timed, elapsed);
          }

          const addr = nonEmpty(ip[i]);
          if (addr !== null && s !== null) {
            if (s === 404) {
              const x = aggOf(insIp, addr);
              x.count += 1;
              x.requests += 1;
            } else if (s === 401 || s === 403) {
              const target = nonEmpty(typeof url[i] === "string" && urlKind === "s" ? (url[i] as string) : null);
              if (target !== null && containsAuthToken(target)) {
                const x = aggOf(insIp, addr);
                x.errors += 1;
                x.requests += 1;
              }
            }
          }

          const latCityName = nonEmpty(plainCity[i]);
          if (latCityName !== null) {
            const x = aggOf(latCity, JSON.stringify([latCityName, region[i] ?? null, plainCountry[i] ?? null]));
            x.requests += 1;
            addElapsed(x, elapsed);
          }
          const popName = nonEmpty(pop[i]);
          if (popName !== null) {
            const x = aggOf(latPop, popName);
            x.requests += 1;
            addElapsed(x, elapsed);
          }
          const proxy = nonEmpty(pType[i]);
          const asnKey = proxy === null ? null : fieldKey(asnKind, asn[i]);
          if (asnKey !== null) {
            const x = aggOf(insAsn, asnKey);
            x.requests += 1;
            if (proxy === "hosting") x.count += 1;
          }
        }
      },
      finish(): Rollup {
        return {
          dims: new Map([["city", cities]]),
          series: new Map([["overall", new Map([["", overall]])]]),
          seriesStepMs: SERIES_STEP_MS,
          fields,
          truncated: new Set(),
          groups: new Map([
            ["ins_url", new Map([["url", insUrl]])],
            ["ins_ip", new Map([["ip", insIp]])],
            ["ins_lat", new Map([["city", latCity], ["pop", latPop]])],
            ["ins_asn", new Map([["asn", insAsn]])],
            ["urls", new Map([["url", urls]])],
          ]),
          present: new Set(PRESENT),
        };
      },
    };
  },
};
